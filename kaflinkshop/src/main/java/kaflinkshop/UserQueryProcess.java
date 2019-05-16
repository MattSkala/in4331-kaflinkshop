package kaflinkshop;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserQueryProcess
        extends KeyedProcessFunction<Tuple, Tuple3<String, String, String>, String> {

    /** The state that is maintained by this process function */
    private ValueState<UserState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", UserState.class));
    }

    @Override
    public void processElement(
            Tuple3<String, String, String> value,
            Context ctx,
            Collector<String> out) throws Exception {

        // retrieve the current count
        UserState current = state.value();
        if (current == null) {
            if(!value.f1.equals("create_user")){
                out.collect(new Tuple3<>("Error","",Integer.toUnsignedLong(0)).toString());
                return;
            }
            current = new UserState();
            current.id = value.f0;
            current.name = value.f2;
        } else {
            // update the state's count
            current.logins++;
        }

        // set the state's timestamp to the record's assigned event time timestamp

        out.collect((new Tuple3<>(current.id, current.name, current.logins)).toString());

        // write the state back
        state.update(current);
    }
}