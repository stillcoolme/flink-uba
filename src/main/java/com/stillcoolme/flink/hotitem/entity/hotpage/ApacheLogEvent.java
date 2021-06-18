package com.stillcoolme.flink.hotitem.entity.hotpage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLogEvent {

    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;

}
