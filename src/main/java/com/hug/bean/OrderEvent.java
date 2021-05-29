package com.hug.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;

    public static OrderEvent toBean(String line) {
        String[] split = line.split(",");
        return new OrderEvent(Long.parseLong(split[0]),
                split[1],
                split[2],
                Long.parseLong(split[3]));
    }
}
