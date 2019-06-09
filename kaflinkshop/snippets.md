# Snippets

This file contains (perhaps) useful code snippets, that we had commented in the code. These have been removed from the code and placed here, instead.


## Checkpoints

This snippet can be used to enabled checkpoints.

```java
public class UserJob { 
	public static void main(String[] args) throws Exception {
	    // set up the streaming execution environment
	    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
	    String filebackend = "file:///root/Documents/TU_Delft/WebData/rocksDB/";
	    String savebackend = "file:///root/Documents/TU_Delft/WebData/saveDB/";
	    
	    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
	    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
	    
	    Configuration config = new Configuration();
	    config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, filebackend);
	    
	    /*
	     * Task local recovery can be enabled, the idea is here:
	     * https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/large_state_tuning.html
	     */
	    
	    config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
	    config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
	    config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savebackend);

	    RocksDBStateBackendFactory factory = new RocksDBStateBackendFactory();
	    StateBackend backend = factory.createFromConfig(config, null);

	    env.enableCheckpointing(10000);
	    env.setStateBackend(backend);
	    
	    // process environment as usual
	}
}
```


## Timers

This snippet was used in `KeyedProcessFunction`, which makes use of timers.

```java
public class OrderQueryProcessOld extends KeyedProcessFunction<K, I, O> {
	
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<O> out) throws Exception {
    
        // get the state for the key that scheduled the timer
        OrderState result = state.value();
    
        // check if this is an outdated timer or the latest timer
        if (!result.check_user) {
            // emit the state on timeout
            System.out.println("Have not checked this order yet!");
        }
    }

}
``` 
