package com.zhuinden.sparkexperiment;

import java.io.Serializable;


public class Schedule implements Serializable {
    private String carrier_cd;

    public Schedule() {
    }

    public Schedule(String carrier_cd) {
        this.carrier_cd=carrier_cd;
    }

    public String getCarrier_cd() {
        return carrier_cd;
    }

    public void setCarrier_cd(String carrier_cd) {
        this.carrier_cd = carrier_cd;
    }



}

