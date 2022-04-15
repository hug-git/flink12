package com.hug.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public static TxEvent toBean(String line) {
        String[] split = line.split(",");
        return new TxEvent(split[0],
                split[1],
                Long.parseLong(split[2]));
    }
}
