package com.reiser.stream.entity.schema;

import com.reiser.stream.entity.AdClientLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des:
 */
public class AdClientLogSchema implements DeserializationSchema<AdClientLog>, SerializationSchema<AdClientLog> {
    @Override
    public AdClientLog deserialize(byte[] bytes) throws IOException {
        return AdClientLog.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(AdClientLog adClientLog) {
        return false;
    }

    @Override
    public byte[] serialize(AdClientLog adClientLog) {
        return new byte[0];
    }

    @Override
    public TypeInformation<AdClientLog> getProducedType() {
        return TypeInformation.of(new TypeHint<AdClientLog>() {
        });
    }
}
