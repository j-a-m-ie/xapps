/*
 * FORCE_STRING_INTERVAL_V1
 */

#include "../../../../src/xApp/e42_xapp_api.h"
#include "../../../../src/util/conf_file.h"
#include "../../../../src/sm/mac_sm/mac_sm_id.h"

#include <assert.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

static volatile sig_atomic_t stop_requested = 0;
static volatile sig_atomic_t interrupt_count = 0;

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
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
}

static void wait_ms_interruptible(int total_ms)
{
  const int step_ms = 100;
  int waited_ms = 0;
  while (!stop_requested && waited_ms < total_ms) {
    usleep(step_ms * 1000);
    waited_ms += step_ms;
  }
}

static void mac_cb(sm_ag_if_rd_t const *rd)
{
  assert(rd != NULL);
  assert(rd->type == INDICATION_MSG_AGENT_IF_ANS_V0);
  assert(rd->ind.type == MAC_STATS_V0);

  mac_ind_msg_t const *msg = &rd->ind.mac.msg;
  for (uint32_t i = 0; i < msg->len_ue_stats; i++) {
    mac_ue_stats_impl_t const *ue = &msg->ue_stats[i];
    printf("UE RNTI %x | Beam ID: %d | Beam Gain: %.1f dB | Num Beams: %u | Beam Map: 0x%lx\n",
           ue->rnti,
           ue->active_beam_id,
           ue->beam_gain_db,
           ue->num_configured_beams,
           (unsigned long)ue->beam_map);
  }
}

int main(int argc, char *argv[])
{
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
  fprintf(stderr, "FORCE_STRING_INTERVAL_V1 interval='%s' ptr=%p\n", interval, (void*)interval);
  fflush(stderr);

  sm_ans_xapp_t h = report_sm_xapp_api(&nodes.n[0].id, SM_MAC_ID, (void*)interval, mac_cb);
  assert(h.success == true);

  while (!stop_requested)
    wait_ms_interruptible(1000);

  if (h.u.handle > 0)
    rm_report_sm_xapp_api(h.u.handle);

  free_e2_node_arr_xapp(&nodes);

  while (try_stop_xapp_api() == false)
    usleep(1000);

  return 0;
}
