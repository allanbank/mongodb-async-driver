/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.expression.Expressions.add;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.divide;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.mod;
import static com.allanbank.mongodb.builder.expression.Expressions.multiply;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static com.allanbank.mongodb.builder.expression.Expressions.subtract;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * AggregateTest provides tests for the example usage of the {@link Aggregate}
 * class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregateTest {

    /**
     * Test method for {@link Aggregate.Builder} usability.
     */
    @Test
    public void testUsability() {

        final int interval = 100000;

        // Example form MongoDB users group message.
        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.match(
                where("calledPloidy").notIn(constant("N")).and("sampleName")
                        .equals("XYZ"))

                .project(
                        include("chr", "begin", "end", "calledPloidy"),
                        set("window",
                                multiply(
                                        divide(subtract(
                                                field("begin"),
                                                mod(field("begin"),
                                                        constant(interval))),
                                                constant(interval)),
                                        constant(interval))))

                .group(id().addField("chr").addField("window"),
                        set("averagePloidy").average("calledPloidy"))

                .project(includeWithoutId(), set("chr", field("_id.chr")),
                        set("begin", field("_id.window")),
                        set("calledPloidy", field("averagePloidy")),
                        set("step", add(constant(0), constant(interval))));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        DocumentBuilder db = expected.push().push("$match");
        db.push("calledPloidy").pushArray("$nin").addString("N");
        db.addString("sampleName", "XYZ");

        db = expected.push().push("$project");
        db.addInteger("chr", 1);
        db.addInteger("begin", 1);
        db.addInteger("end", 1);
        db.addInteger("calledPloidy", 1);
        final ArrayBuilder mult = db.push("window").pushArray("$multiply");
        final ArrayBuilder div = mult.push().pushArray("$divide");
        final ArrayBuilder sub = div.push().pushArray("$subtract");
        sub.addString("$begin");
        sub.push().pushArray("$mod").addString("$begin").addInteger(interval);
        div.addInteger(interval);
        mult.addInteger(interval);

        db = expected.push().push("$group");
        db.push("_id").addString("chr", "$chr").addString("window", "$window");
        db.push("averagePloidy").addString("$avg", "$calledPloidy");

        db = expected.push().push("$project");
        db.addInteger("_id", 0);
        db.addString("chr", "$_id.chr");
        db.addString("begin", "$_id.window");
        db.addString("calledPloidy", "$averagePloidy");
        db.push("step").pushArray("$add").addInteger(0).addInteger(interval);

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());
    }
}
