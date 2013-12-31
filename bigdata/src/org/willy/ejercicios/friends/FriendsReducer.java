package org.willy.ejercicios.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.willy.ejercicios.friends.writable.FriendsRelationsWritable;

import java.io.IOException;
import java.util.ArrayList;

public class FriendsReducer extends Reducer<Text, FriendsRelationsWritable, Text, Text> {

    @Override
    protected void reduce(Text keyFriend, Iterable<FriendsRelationsWritable> relations, Context context) throws IOException, InterruptedException {
        ArrayList<Text> directRelationships = new ArrayList<Text>();
        ArrayList<Text> reverseRelationships = new ArrayList<Text>();

        for (FriendsRelationsWritable relation : relations) {
            FriendsRelationsWritable copy = WritableUtils.clone(relation, context.getConfiguration());
            if (copy.isRightRelationship()) {
                FriendsRelationsWritable now = copy;
                directRelationships.add(copy.getFriend());
            } else {
                reverseRelationships.add(copy.getFriend());
            }
        }

        for (Text originFriend : reverseRelationships) {
            for (Text destinyFriend : directRelationships) {
                if (!originFriend.equals(destinyFriend)) context.write(originFriend,destinyFriend);
            }
        }

    }
}
