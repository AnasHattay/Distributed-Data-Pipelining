package fr.cnam.cedric.roc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class Reducer implements FlatMapFunction<Tuple2<String, Integer>, String>{

	@Override
	public void flatMap(Tuple2<String, Integer> value, Collector<String> out) throws Exception {
		// TODO Auto-generated method stub
		String count = value.f0 + " " + value.f1;
    	out.collect(count);
	}

}
