package com.hug.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public static UserBehavior toBean(String line) {
        String[] split = line.split(",");
        return new UserBehavior(Long.parseLong(split[0]),
                Long.parseLong(split[1]),
                Integer.parseInt(split[2]),
                split[3],
                Long.parseLong(split[4]));
    }
}
