package com.hug.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;

    public static LoginEvent toBean(String line) {
        String[] split = line.split(",");
        return new LoginEvent(Long.parseLong(split[0]),
                split[1],
                split[2],
                Long.parseLong(split[3]));
    }
}
