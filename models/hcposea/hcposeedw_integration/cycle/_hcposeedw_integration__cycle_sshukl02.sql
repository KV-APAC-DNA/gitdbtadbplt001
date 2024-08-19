version: 2

models:
  - name: hcposeedw_integration__fact_cycle_plan
    config:
      alias: fact_cycle_plan
      tags: ["edw_load","transformation"]
  - name: hcposeedw_integration__wrk_cycle_plan
    config:
      alias: wrk_cycle_plan
      tags: ["edw_load","transformation"]
  - name: hcposeedw_integration__vw_rep_cycle_plan
    config:
      alias: vw_rep_cycle_plan
      tags: ["hcp_views","transformation"]