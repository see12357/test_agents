# OS Update Manual for Patroni Clusters

When performing operating system updates on Patroni clusters hosting PostgreSQL databases, follow these steps:

1. Put the cluster into maintenance (pause) mode:
   ```bash
   patronictl -c /etc/patroni/patroni.yml pause
   ```
2. Update operating system packages on the replicas one by one:
   ```bash
   apt-get update && apt-get upgrade -y
   ```
3. Reboot the replicas and verify that they sync back with the master:
   ```bash
   reboot
   ```
4. Perform a manual leader switchover from the master to an updated replica:
   ```bash
   patronictl -c /etc/patroni/patroni.yml switchover
   ```
5. Update the OS on the former master node and resume normal cluster operations:
   ```bash
   patronictl -c /etc/patroni/patroni.yml resume
   ```
