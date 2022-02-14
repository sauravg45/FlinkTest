package test1;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;

public class WordCount
{

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/saurabhg/Documents/ip_data.txt");


        // click data keyed by website
        DataStream<Tuple2<String, String>> keyedData =	  data
                .map(new MapFunction<String, Tuple2<String, String>>()
                {
                    public Tuple2<String, String> map(String value)
                    {
                        String[] words = value.split(",");
                        // <website, all_data>
                        return new Tuple2<String, String>(words[4], value);
                    }
                });

        // US click stream only
        DataStream<Tuple2<String, String>> usStream =	 keyedData.filter(new FilterFunction<Tuple2<String, String>>()
        {
            public boolean filter(Tuple2<String, String> value)
            {
                String country = value.f1.split(",")[3];
                return !country.equals("US");
            }
        });

        // total number of clicks on every website in US
        DataStream<Tuple2<String, Integer>> clicksPerWebsite = usStream .map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>()
                {
                    public Tuple3<String, String, Integer> map(Tuple2<String, String> value)
                    {
                        return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
                    }
                })
                .keyBy(0)
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>()
                {
                    public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value)
                    {
                        return new Tuple2<String, Integer>(value.f0, value.f2);
                    }
                });
        clicksPerWebsite.writeAsText("/Users/saurabhg/Documents/clicks_per_web.txt");

        // website with max clicks
        DataStream<Tuple2<String, Integer>> maxClicks =	 clicksPerWebsite
                .keyBy(0)
                .maxBy(1);

        maxClicks.writeAsText("/Users/saurabhg/Documents/max_clicks.txt");

        // website with min clicks
        DataStream<Tuple2<String, Integer>> minClicks =
                clicksPerWebsite
                        .keyBy(0)
                        .minBy(1);

        minClicks.writeAsText("/Users/saurabhg/Documents/min_clicks.txt");
        DataStream<Tuple2<String, Integer>> avgTimeWebsite =
                usStream
                        .map(new MapFunction<Tuple2<String, String>, Tuple3<String, Integer, Integer>>(){
                            public Tuple3<String, Integer, Integer> map(Tuple2<String, String> value){
                                int timeSpent = Integer.parseInt(value.f1.split(",")[5]);
                                return new Tuple3<String, Integer, Integer>(value.f0, 1, timeSpent);
                            }
                        })
                        .keyBy(0)
                        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                        .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>()
                        {
                            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1,
                                                                           Tuple3<String, Integer, Integer> v2)
                            {
                                return new Tuple3<String, Integer, Integer>(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2);
                            }
                        })
                        .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>(){
                            public Tuple2<String, Integer> map(Tuple3<String, Integer, Integer> value){
                                return new Tuple2<String, Integer>(value.f0, (value.f2/value.f1));
                            }
                        });
        avgTimeWebsite.writeAsText("/Users/saurabhg/Documents/avg_per_web.txt");

        // distinct users on each website
        DataStream<Tuple2<String, Integer>> usersPerWebsite =	 usStream
                .keyBy(0)
                .flatMap(new DistinctUsers());

        usersPerWebsite.writeAsText("/Users/saurabhg/Documents/distinct_users.txt");

        // execute program
        env.execute("Streaming Click");
    }

    public static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>
    {
        private transient ListState<String> usersState;

        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception{
            usersState.add(input.f1);

            HashSet<String> distinctUsers = new HashSet<String>();
            for (String user : usersState.get())
            {
                distinctUsers.add(user);
            }
            out.collect(new Tuple2<String, Integer>(input.f0, distinctUsers.size()));
        }

        public void open(Configuration conf)
        {
            ListStateDescriptor<String> desc = new ListStateDescriptor<String>("users_state", BasicTypeInfo.STRING_TYPE_INFO);
            usersState = getRuntimeContext().getListState(desc);
        }
    }
}
