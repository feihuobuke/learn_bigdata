package com.reiser.stream.ad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reiser.stream.entity.AdLog;
import com.reiser.stream.entity.dto.AdLogDTO;
import com.reiser.stream.untils.ETLUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/11/21
 * Des:
 */
public class AdLogRichFlatMap extends RichFlatMapFunction<AdLog, String> {
    ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {

        objectMapper = new ObjectMapper();

        super.open(parameters);
    }

    @Override
    public void flatMap(AdLog adLog, Collector<String> collector) throws Exception {
        AdLogDTO adLogDTO = ETLUtils.buildAdLogDTO(adLog);
        collector.collect(objectMapper.writeValueAsString(adLogDTO));
    }
}
