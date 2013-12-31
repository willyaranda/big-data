package org.willy.ejercicios.friends.writable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendsRelationsWritable implements WritableComparable<FriendsRelationsWritable> {

    private BooleanWritable type;
    private Text friend;

    public FriendsRelationsWritable() {
        this.type = new BooleanWritable();
        this.friend = new Text();
    }

    public FriendsRelationsWritable(BooleanWritable type, Text friend) {
        this.type = type;
        this.friend = new Text(friend);
    }

    public BooleanWritable getType() {
        return type;
    }

    public void setType(BooleanWritable type) {
        this.type = type;
    }

    public Text getFriend() {
        return friend;
    }

    public void setFriend(Text friend) {
        this.friend = friend;
    }

    public boolean isRightRelationship() {
        return this.type.equals(true);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        friend.write(dataOutput);
        type.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        friend.readFields(dataInput);
        type.readFields(dataInput);
    }

    @Override
    public int compareTo(FriendsRelationsWritable FriendsRelationsWritable) {
        return (friend.compareTo(FriendsRelationsWritable.getFriend()) + type.compareTo(FriendsRelationsWritable.type));
    }

}