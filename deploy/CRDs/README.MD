These CRDs cover all resource types for Simplyblock operator.
Apart from the standard operations (create, get, update, delete), some have additional
actions, specified in the "action" field.
E.g. Backups can be restored, clusters can be activated and upgraded, 
storage nodes can be restarted and suspended
replications can be stopped, resumed, failed-over and failed-back

Packaging of containers into PODs:
1. simplyblock-kvs
foundationdb + coordinator
app_fdb-server.ivs5fjl8se44qa60frmsxw2fb.k8dtfdxajzuu5n7srvlijmikw
app_fdb-backup-agent.1.ar76tui5nfit166er9zrgugz0
app_CleanupFDB.1.n6a8y25ckfjxhpl0umooz8axk
???


2. simpblyblock-observability
ENTIRE log mgmt. and monitoring stack without prometheus (can be disabled!)
monitoring_opensearch.1.e4ism2jaimd9ljj11tw0jm8am
monitoring_graylog.1.wf7qnrgo5mjnyxnomdntht7g7
monitoring_grafana.1.ijf8z9c2ap8lcddashqgbj7et
monitoring_thanos-sidecar.ivs5fjl8se44qa60frmsxw2fb.tqeblhqducajyee0bzbmjbt90
e44qa60frmsxw2fb.14e0arjdgr7vne6jsvfutr6z9
monitoring_thanos-query.ivs5fjl8se44qa60frmsxw2fb.qbttzex5b4zu6hhf7tuecghty
thanosio/thanos:v0.31.0 
monitoring_node-exporter.ivs5fjl8se44qa60frmsxw2fb.qveinv2rbl4ph2aje66jzq50u

3. simplyblock-WebAPI
Include Prometheus

4. simplyblock-services
pp_LVolStatsCollector.1.8m1p8tpkkvsphxc89rf4qor51
app_MgmtNodeMonitor.1.ol2oury31jde99q3qdawcaljn 
app_TasksRunnerRestart.1.i2ezt6563bnky2gqr7p2d1t54
app_TasksRunnerLVolSyncDelete.1.z17is5c4x65qdrq1o2q6229vh
app_HealthCheck.1.n1cy0ytarz9m1viywftxnas83
app_TasksRunnerJCCompResume.1.drxmxzzufirtaj09be9k87er4
app_SnapshotMonitor.1.nt7vz4hz3slywo1vjcdd577tk
app_DeviceMonitor.1.0jxv23onq5fy5x6ki4ksm61n1
app_TasksRunnerPortAllow.1.zwi8gkxfsr4w92ozsxlviglcd
app_TasksRunnerNewDeviceMigration.1.kjcqjdz9uiroohmbdr53hzmqd
app_MainDistrEventCollector.1.8ub3urb45m7abvvsi522432fr
app_TasksNodeAddRunner.1.mi4xkekchokfc3prwn744cw3l
app_CapacityAndStatsCollector.1.vw55n3kxmnxzn5bnnt77p6exb
app_TasksRunnerFailedMigration.1.8nn9u7uhpupwecq7jumrqley9
app_TasksRunnerClusterStatus.1.nuifhdq985efldlzyxfce8747
app_StorageNodeMonitor.1.q430g2vfpz2gambbf18arb4c8
app_LVolMonitor.1.le2sqbw3ebgklkan2gf5fylzl
app_TasksRunnerMigration.1.vl19rxbnquuct0t0vej6runou
app_CapacityMonitor.1.z2euv14ng5xfmbdjv1jpse6e1





     Up 13 hours                                                      monitoring_thanos-compactor.ivs5fjl8se44qa60frmsxw2fb.8dnumd5yc292529qpcqki5x