package com.tt.kafka.push.server.service;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RegisterMetadata implements Serializable {

    private String topic;

    private Address address;

    public RegisterMetadata(){
        //
    }

    public RegisterMetadata(String topic, Address address){
        this.topic = topic;
        this.address = address;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "RegisterMetadata{" +
                "topic='" + topic + '\'' +
                ", address=" + address +
                '}';
    }

    public static class Address implements Serializable {

        public Address(){

        }

        public Address(String host, int port){
            this.host = host;
            this.port = port;
        }

        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((host == null) ? 0 : host.hashCode());
            result = prime * result + port;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Address other = (Address) obj;
            if (host == null) {
                if (other.host != null)
                    return false;
            } else if (!host.equals(other.host))
                return false;
            if (port != other.port)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Address [host=" + host + ", port=" + port + "]";
        }

    }
}
