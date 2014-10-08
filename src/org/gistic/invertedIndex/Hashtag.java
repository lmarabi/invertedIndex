/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.invertedIndex;

/**
 *
 * @author turtle
 */
public class Hashtag {

    public String lat;
    public String lon;
    public String hashtagText;

    public Hashtag(String hashtagLine) {
        String[] token = hashtagLine.split(",");
        this.lat = token[0];
        this.lon = token[1];
        this.hashtagText = token[2];
    }

    @Override
    public String toString() {
        return lat + "," + lon + "," + hashtagText;
    }
    
    
    
    

}
