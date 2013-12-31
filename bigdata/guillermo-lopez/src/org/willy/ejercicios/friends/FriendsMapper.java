package org.willy.ejercicios.friends;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.willy.ejercicios.friends.writable.FriendsRelationsWritable;

import java.io.IOException;

public class FriendsMapper extends Mapper<Text, Text, Text, FriendsRelationsWritable> {

    @Override
    protected void map(Text leftFriend, Text rightFriend, Context context) throws IOException, InterruptedException {
    	System.out.println("Left="+leftFriend+", right="+rightFriend);
        if (leftFriend.toString().isEmpty() || rightFriend.toString().isEmpty())
            return;
        context.write(leftFriend, new FriendsRelationsWritable(new BooleanWritable(true), rightFriend));
        context.write(rightFriend, new FriendsRelationsWritable(new BooleanWritable(false), leftFriend));
    }
}