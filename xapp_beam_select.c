#include "../../../../src/xApp/e42_xapp_api.h"
#include "../../../../src/util/conf_file.h"
#include "../../../../src/sm/mac_sm/mac_sm_id.h"

#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_BEAMS 64

static volatile sig_atomic_t stop_requested = 0;
static volatile sig_atomic_t interrupt_count = 0;

typedef struct {
  pthread_mutex_t mtx;
  uint64_t total_samples;
  double gain_sum[MAX_BEAMS];
  uint64_t gain_cnt[MAX_BEAMS];
  int last_active_beam;
  double last_gain_db;
  unsigned last_num_beams;
  uint64_t last_beam_map;
} obs_state_t;

typedef struct {
  bool valid;
  int beam_id;
  double avg_gain_db;
  uint64_t n;
  int last_active_beam;
  unsigned last_num_beams;
  uint64_t last_beam_map;
} probe_result_t;

static obs_state_t g_obs = {
  .mtx = PTHREAD_MUTEX_INITIALIZER,
  .total_samples = 0,
  .last_active_beam = -1,
  .last_gain_db = 0.0,
  .last_num_beams = 0,
  .last_beam_map = 0
};

static void signal_handler(int sig)
{
  (void)sig;
  stop_requested = 1;
  interrupt_count++;
  if (interrupt_count >= 2)
    _exit(130);
}

static void install_signal_handlers(void)
{
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
}

static void wait_ms_interruptible(int total_ms)
{
  const int step_ms = 50;
  int waited = 0;
  while (!stop_requested && waited < total_ms) {
    usleep(step_ms * 1000);
    waited += step_ms;
  }
}

static int env_int(const char* name, int def_val)
{
  const char* s = getenv(name);
  if (s == NULL || *s == '\0')
    return def_val;

  char* end = NULL;
  long v = strtol(s, &end, 10);
  if (end == s || *end != '\0')
    return def_val;

  return (int)v;
}

/* Blocking IPv4 connect path to match common telnet/netcat behavior on 127.0.0.1 */
static int connect_tcp_blocking_v4(const char* host, int port)
{
  char port_str[16];
  snprintf(port_str, sizeof(port_str), "%d", port);

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_NUMERICSERV;

  struct addrinfo* res = NULL;
  int gai = getaddrinfo(host, port_str, &hints, &res);
  if (gai != 0) {
    fprintf(stderr, "[xApp] getaddrinfo(%s:%d) failed: %s\n", host, port, gai_strerror(gai));
    return -1;
  }

  int fd = -1;
  int last_errno = ECONNREFUSED;

  for (struct addrinfo* rp = res; rp != NULL; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0) {
      last_errno = errno;
      continue;
    }

    int rc;
    do {
      rc = connect(fd, rp->ai_addr, (socklen_t)rp->ai_addrlen);
    } while (rc < 0 && errno == EINTR && !stop_requested);

    if (rc == 0) {
      freeaddrinfo(res);
      return fd;
    }

    last_errno = errno;
    close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  errno = last_errno;
  return -1;
}

static int tcp_send_line(const char* host, int port, const char* line, int io_timeout_ms, int retries)
{
  if (retries < 1)
    retries = 1;

  for (int attempt = 1; attempt <= retries && !stop_requested; ++attempt) {
    int fd = connect_tcp_blocking_v4(host, port);
    if (fd < 0) {
      if (attempt == retries) {
        fprintf(stderr, "[xApp] connect(%s:%d) failed: %s\n", host, port, strerror(errno));
      } else {
        wait_ms_interruptible(100);
      }
      continue;
    }

    struct timeval tv = { io_timeout_ms / 1000, (io_timeout_ms % 1000) * 1000 };
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buf[256];
    int n = snprintf(buf, sizeof(buf), "%s\r\n", line);
    if (n <= 0 || n >= (int)sizeof(buf)) {
      close(fd);
      errno = EINVAL;
      return -1;
    }

    int sent = 0;
    int send_err = 0;
    while (sent < n) {
      ssize_t rc = send(fd, buf + sent, (size_t)(n - sent), 0);
      if (rc < 0) {
        if (errno == EINTR)
          continue;
        send_err = errno;
        break;
      }
      if (rc == 0) {
        send_err = EPIPE;
        break;
      }
      sent += (int)rc;
    }

    (void)shutdown(fd, SHUT_WR);

    /* Best-effort drain so server can finish cleanly */
    char drain[256];
    while (recv(fd, drain, sizeof(drain), 0) > 0)
      ;

    close(fd);

    if (send_err == 0 && sent == n)
      return 0;

    errno = (send_err != 0) ? send_err : EIO;

    if (attempt < retries)
      wait_ms_interruptible(100);
  }

  return -1;
}

static int set_gnb_beam(const char* host, int port, int beam_id, int io_timeout_ms, int retries)
{
  char cmd[64];
  snprintf(cmd, sizeof(cmd), "rfsimu setbeamids %d", beam_id);
  return tcp_send_line(host, port, cmd, io_timeout_ms, retries);
}

static void mac_cb(sm_ag_if_rd_t const *rd)
{
  if (rd == NULL)
    return;
  if (rd->type != INDICATION_MSG_AGENT_IF_ANS_V0)
    return;
  if (rd->ind.type != MAC_STATS_V0)
    return;

  mac_ind_msg_t const* msg = &rd->ind.mac.msg;

  pthread_mutex_lock(&g_obs.mtx);
  for (uint32_t i = 0; i < msg->len_ue_stats; ++i) {
    mac_ue_stats_impl_t const* ue = &msg->ue_stats[i];
    int b = ue->active_beam_id;

    if (b >= 0 && b < MAX_BEAMS) {
      g_obs.gain_sum[b] += ue->beam_gain_db;
      g_obs.gain_cnt[b] += 1;
    }

    g_obs.total_samples += 1;
    g_obs.last_active_beam = ue->active_beam_id;
    g_obs.last_gain_db = ue->beam_gain_db;
    if (ue->num_configured_beams > 0)
      g_obs.last_num_beams = ue->num_configured_beams;
    g_obs.last_beam_map = ue->beam_map;
  }
  pthread_mutex_unlock(&g_obs.mtx);
}

static void snapshot_beam(int beam,
                          uint64_t* cnt,
                          double* sum,
                          int* last_active,
                          unsigned* last_num_beams,
                          uint64_t* last_beam_map)
{
  pthread_mutex_lock(&g_obs.mtx);
  *cnt = (beam >= 0 && beam < MAX_BEAMS) ? g_obs.gain_cnt[beam] : 0;
  *sum = (beam >= 0 && beam < MAX_BEAMS) ? g_obs.gain_sum[beam] : 0.0;
  *last_active = g_obs.last_active_beam;
  *last_num_beams = g_obs.last_num_beams;
  *last_beam_map = g_obs.last_beam_map;
  pthread_mutex_unlock(&g_obs.mtx);
}

static void snapshot_global(uint64_t* total_samples, unsigned* last_num_beams, uint64_t* last_beam_map)
{
  pthread_mutex_lock(&g_obs.mtx);
  *total_samples = g_obs.total_samples;
  *last_num_beams = g_obs.last_num_beams;
  *last_beam_map = g_obs.last_beam_map;
  pthread_mutex_unlock(&g_obs.mtx);
}

static int build_beam_list(int out[MAX_BEAMS],
                           int beam_count_cfg,
                           int beam_id_base,
                           bool use_beam_map,
                           uint64_t obs_bmap)
{
  int n = 0;

  if (use_beam_map && obs_bmap != 0) {
    for (int b = 0; b < MAX_BEAMS; ++b) {
      if ((obs_bmap & (1ULL << b)) != 0)
        out[n++] = b;
    }
    return n;
  }

  for (int i = 0; i < beam_count_cfg; ++i) {
    int b = beam_id_base + i;
    if (b < 0 || b >= MAX_BEAMS)
      continue;
    out[n++] = b;
  }

  return n;
}

static probe_result_t probe_beam(const char* host,
                                 int port,
                                 int beam_id,
                                 int settle_ms,
                                 int dwell_ms,
                                 int min_samples,
                                 int io_timeout_ms,
                                 int ctrl_retries)
{
  probe_result_t r = {0};
  r.beam_id = beam_id;
  r.last_active_beam = -1;

  if (set_gnb_beam(host, port, beam_id, io_timeout_ms, ctrl_retries) != 0) {
    fprintf(stderr, "[scan] beam %d: setbeamids failed\n", beam_id);
    return r;
  }

  wait_ms_interruptible(settle_ms);
  if (stop_requested)
    return r;

  uint64_t c0 = 0, c1 = 0;
  double s0 = 0.0, s1 = 0.0;
  unsigned nbeams = 0;
  uint64_t bmap = 0;
  int last_active = -1;

  snapshot_beam(beam_id, &c0, &s0, &last_active, &nbeams, &bmap);

  int waited = 0;
  while (!stop_requested && waited < dwell_ms) {
    wait_ms_interruptible(50);
    waited += 50;

    snapshot_beam(beam_id, &c1, &s1, &last_active, &nbeams, &bmap);
    if (c1 >= c0 + (uint64_t)min_samples)
      break;
  }

  snapshot_beam(beam_id, &c1, &s1, &last_active, &nbeams, &bmap);

  r.last_active_beam = last_active;
  r.last_num_beams = nbeams;
  r.last_beam_map = bmap;

  if (c1 <= c0)
    return r;

  r.valid = true;
  r.n = c1 - c0;
  r.avg_gain_db = (s1 - s0) / (double)r.n;
  return r;
}

int main(int argc, char *argv[])
{
  const char* ctrl_host = getenv("BEAM_CTRL_HOST");
  if (ctrl_host == NULL || *ctrl_host == '\0')
    ctrl_host = "127.0.0.1";

  int ctrl_port = env_int("BEAM_CTRL_PORT", 9090);
  int beam_count_cfg = env_int("BEAM_COUNT", 4);
  int beam_id_base = env_int("BEAM_ID_BASE", 0);

  int settle_ms = env_int("BEAM_SETTLE_MS", 250);
  int dwell_ms = env_int("BEAM_DWELL_MS", 900);
  int min_samples = env_int("BEAM_MIN_SAMPLES", 20);
  int rescan_ms = env_int("BEAM_RESCAN_MS", 3000);

  int io_timeout_ms = env_int("BEAM_CTRL_IO_MS", 250);
  int ctrl_retries = env_int("BEAM_CTRL_RETRIES", 2);

  /* Default: scan full configured range (fix for stopping at one mapped beam). */
  bool use_beam_map = (env_int("BEAM_USE_BEAM_MAP", 0) != 0);

  if (beam_count_cfg < 1) beam_count_cfg = 1;
  if (beam_count_cfg > MAX_BEAMS) beam_count_cfg = MAX_BEAMS;
  if (beam_id_base < 0) beam_id_base = 0;
  if (beam_id_base >= MAX_BEAMS) beam_id_base = MAX_BEAMS - 1;
  if (min_samples < 1) min_samples = 1;
  if (io_timeout_ms < 50) io_timeout_ms = 50;
  if (ctrl_retries < 1) ctrl_retries = 1;
  if (ctrl_retries > 10) ctrl_retries = 10;

  fr_args_t args = init_fr_args(argc, argv);
  init_xapp_api(&args);
  install_signal_handlers();

  e2_node_arr_xapp_t nodes = {0};
  while (!stop_requested) {
    nodes = e2_nodes_xapp_api();
    if (nodes.len > 0)
      break;

    free_e2_node_arr_xapp(&nodes);
    wait_ms_interruptible(500);
  }

  if (stop_requested) {
    while (try_stop_xapp_api() == false)
      usleep(1000);
    return 0;
  }

  const char *interval = "1_ms";
  sm_ans_xapp_t h = report_sm_xapp_api(&nodes.n[0].id, SM_MAC_ID, (void*)interval, mac_cb);
  if (!h.success) {
    fprintf(stderr, "[xApp] MAC subscription failed\n");
    free_e2_node_arr_xapp(&nodes);
    while (try_stop_xapp_api() == false)
      usleep(1000);
    return 1;
  }

  fprintf(stderr, "[xApp] selector start ctrl=%s:%d\n", ctrl_host, ctrl_port);

  int warmup = 5000;
  while (!stop_requested && warmup > 0) {
    uint64_t total_samples = 0;
    unsigned dummy_nbeams = 0;
    uint64_t dummy_bmap = 0;
    snapshot_global(&total_samples, &dummy_nbeams, &dummy_bmap);
    if (total_samples > 0)
      break;

    wait_ms_interruptible(100);
    warmup -= 100;
  }

  while (!stop_requested) {
    uint64_t total_samples = 0;
    unsigned obs_nbeams = 0;
    uint64_t obs_bmap = 0;
    snapshot_global(&total_samples, &obs_nbeams, &obs_bmap);

    if (total_samples == 0) {
      fprintf(stderr, "[scan] no MAC samples yet, waiting...\n");
      wait_ms_interruptible(500);
      continue;
    }

    int beams_to_scan[MAX_BEAMS];
    int scan_n = build_beam_list(beams_to_scan, beam_count_cfg, beam_id_base, use_beam_map, obs_bmap);

    if (scan_n <= 0) {
      fprintf(stderr, "[scan] no beams in scan list (cfg=%d base=%d map=0x%llx)\n",
              beam_count_cfg, beam_id_base, (unsigned long long)obs_bmap);
      wait_ms_interruptible(500);
      continue;
    }

    fprintf(stderr, "\n[scan] scanning %d beam(s), mode=%s, map=0x%llx\n",
            scan_n, use_beam_map ? "map" : "full",
            (unsigned long long)obs_bmap);

    probe_result_t best = {0};

    for (int i = 0; i < scan_n && !stop_requested; ++i) {
      int b = beams_to_scan[i];

      probe_result_t r = probe_beam(ctrl_host, ctrl_port, b, settle_ms, dwell_ms,
                                    min_samples, io_timeout_ms, ctrl_retries);

      if (!r.valid) {
        fprintf(stderr, "[scan] beam %d -> no fresh samples (last_active=%d map=0x%llx)\n",
                b, r.last_active_beam, (unsigned long long)r.last_beam_map);
        continue;
      }

      fprintf(stderr, "[scan] beam %d -> avg_gain=%.2f dB (n=%" PRIu64
                      ", last_active=%d map=0x%llx)\n",
              b, r.avg_gain_db, r.n, r.last_active_beam, (unsigned long long)r.last_beam_map);

      if (!best.valid || r.avg_gain_db > best.avg_gain_db)
        best = r;
    }

    if (best.valid) {
      if (set_gnb_beam(ctrl_host, ctrl_port, best.beam_id, io_timeout_ms, ctrl_retries) == 0) {
        fprintf(stderr, "[decision] selected beam %d (avg_gain=%.2f dB)\n",
                best.beam_id, best.avg_gain_db);
      } else {
        fprintf(stderr, "[decision] failed to apply beam %d\n", best.beam_id);
      }
    } else {
      fprintf(stderr, "[decision] no valid beam this cycle\n");
    }

    wait_ms_interruptible(rescan_ms);
  }

  if (h.u.handle > 0)
    rm_report_sm_xapp_api(h.u.handle);

  free_e2_node_arr_xapp(&nodes);

  while (try_stop_xapp_api() == false)
    usleep(1000);

  return 0;
}
