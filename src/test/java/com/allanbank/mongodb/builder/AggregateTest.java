/*
 * #%L
 * AggregateTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
import static org.junit.Assert.assertSame;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.util.Arrays;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.DocumentElement;

/**
 * AggregateTest provides tests for the example usage of the {@link Aggregate}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 * @deprecated See deprecation of {@link Aggregate}.
 */
@Deprecated
public class AggregateTest {

    /**
     * Test method for {@link Aggregate}.
     */
    @Test
    public void testAggregationWithreadPreference() {
        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.group(id("a"), set("d").average("e"));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        final DocumentBuilder db = expected.push().push("$group");
        db.addString("_id", "$a");
        db.push("d").addString("$avg", "$e");

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());

        assertSame(ReadPreference.PREFER_SECONDARY, builder.build()
                .getReadPreference());
    }

    /**
     * Test the #geoNear pipeline operator.
     */
    @Test
    public void testGeoNear() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear.Builder geoNear = AggregationGeoNear.builder()
                .location(location).distanceField("dist")
                .distanceMultiplier(1.2).limit(123).locationField("loc")
                .maxDistance(12345.6).query(BuilderFactory.start().add("a", 2))
                .spherical().uniqueDocs(false);

        final Aggregate aggregate = Aggregate.builder().geoNear(geoNear)
                .build();

        final DocumentBuilder expected = BuilderFactory.start();
        final DocumentBuilder geoNearOp = expected.push("$geoNear");
        geoNearOp.pushArray("near").add(1).add(2);
        geoNearOp.add("distanceField", "dist");
        geoNearOp.add("spherical", true);
        geoNearOp.add("uniqueDocs", false);

        geoNearOp.add("limit", 123L);
        geoNearOp.add("maxDistance", 12345.6);
        geoNearOp.add("query", BuilderFactory.start().add("a", 2));
        geoNearOp.add("distanceMultiplier", 1.2);
        geoNearOp.add("includeLocs", "loc");

        assertEquals(Arrays.asList(new DocumentElement("0", expected.build())),
                aggregate.getPipeline());

    }

    /**
     * Test method for {@link Aggregate.Builder} usability.
     * 
     * @see <a
     *      href="https://groups.google.com/d/topic/mongodb-user/1cYch580h0w/discussion">Inspired
     *      By</a>
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
