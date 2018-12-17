package com.mpojeda84.maprdb.java.ojai.model;

public class Business {

    public String _id;
    public String aaddress;
    public boolean open;

    public Business(String _id, String aaddress, boolean open) {
        this._id = _id;
        this.aaddress = aaddress;
        this.open = open;
    }
}
