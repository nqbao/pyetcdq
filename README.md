## A worker queue implements with Etcd and Python ##

This is a POC of a distributed task queue framework built on top of etcd. The goal of this is to
provide a reliable and scalalbe framework to build distributed task execution.

### Approach ###

Task submissions:
 * Task submission is done by by creating a file in `tasks`.

Task processing:
 * Worker scans for pending tasks.
 * If there is no pending task, then it starts to watch for changes
   * When there is new task, or there is change in states. Worker will try to acquire for the job by creating a lock filem, since this is an atomic action, only one will get the job
   * Worker executes the task and update status to success when it is done.
   * During the execution, the worker must periodically refresh the ttl
   * On failure, the worker should remove it locks to the file so other workers can acquire the task.

### Questions ###

 * How to handle failures?
 * How to handle "round-robin" behavior?
 * How to cancel a task?: use process interupt
 * Capacity-based worker
 * Task dependencies
 * Task priorities
 * Task retries
 * Error handling


### References ###

 * https://github.com/OpsLabJPL/etcdq
 * https://stackoverflow.com/questions/34629860/how-would-you-implement-a-working-queue-in-etcd
