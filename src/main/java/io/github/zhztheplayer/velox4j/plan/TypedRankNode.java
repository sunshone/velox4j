/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.zhztheplayer.velox4j.plan;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.stateful.KeyedStateSpec;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

/**
 * Java mirror of velox's {@code TypedRankNode}: the typed rank plan node for Flink StreamExecRank
 * (Top-N with ROW_NUMBER output). Carries the keyed-state spec (partition key channels and types),
 * the sort specification, the constant rank range and the changelog knobs. Same name and same
 * fields as the C++ class; the serde registration name is "TypedRankNode" on both sides.
 */
public class TypedRankNode extends PlanNode {

  private final KeyedStateSpec keyedStateSpec;
  private final List<Integer> sortKeyChannels;
  private final List<Type> sortKeyTypes;
  private final List<Boolean> ascendingOrders;
  private final List<Boolean> nullsLasts;
  private final long rankStart;
  private final long rankEnd;
  private final boolean outputRankNumber;
  private final boolean generateUpdateBefore;
  private final boolean appendOnly;
  private final RowType inputType;
  private final RowType outputType;

  @JsonCreator
  public TypedRankNode(
      @JsonProperty("id") String id,
      @JsonProperty("keyedStateSpec") KeyedStateSpec keyedStateSpec,
      @JsonProperty("sortKeyChannels") List<Integer> sortKeyChannels,
      @JsonProperty("sortKeyTypes") List<Type> sortKeyTypes,
      @JsonProperty("ascendingOrders") List<Boolean> ascendingOrders,
      @JsonProperty("nullsLasts") List<Boolean> nullsLasts,
      @JsonProperty("rankStart") long rankStart,
      @JsonProperty("rankEnd") long rankEnd,
      @JsonProperty("outputRankNumber") boolean outputRankNumber,
      @JsonProperty("generateUpdateBefore") boolean generateUpdateBefore,
      @JsonProperty("appendOnly") boolean appendOnly,
      @JsonProperty("inputType") RowType inputType,
      @JsonProperty("outputType") RowType outputType) {
    super(id);
    this.keyedStateSpec = keyedStateSpec;
    this.sortKeyChannels = sortKeyChannels;
    this.sortKeyTypes = sortKeyTypes;
    this.ascendingOrders = ascendingOrders;
    this.nullsLasts = nullsLasts;
    this.rankStart = rankStart;
    this.rankEnd = rankEnd;
    this.outputRankNumber = outputRankNumber;
    this.generateUpdateBefore = generateUpdateBefore;
    this.appendOnly = appendOnly;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  @Override
  protected List<PlanNode> getSources() {
    return List.of();
  }

  @JsonGetter("keyedStateSpec")
  public KeyedStateSpec getKeyedStateSpec() {
    return keyedStateSpec;
  }

  @JsonGetter("sortKeyChannels")
  public List<Integer> getSortKeyChannels() {
    return sortKeyChannels;
  }

  @JsonGetter("sortKeyTypes")
  public List<Type> getSortKeyTypes() {
    return sortKeyTypes;
  }

  @JsonGetter("ascendingOrders")
  public List<Boolean> getAscendingOrders() {
    return ascendingOrders;
  }

  @JsonGetter("nullsLasts")
  public List<Boolean> getNullsLasts() {
    return nullsLasts;
  }

  @JsonGetter("rankStart")
  public long getRankStart() {
    return rankStart;
  }

  @JsonGetter("rankEnd")
  public long getRankEnd() {
    return rankEnd;
  }

  @JsonGetter("outputRankNumber")
  public boolean getOutputRankNumber() {
    return outputRankNumber;
  }

  @JsonGetter("generateUpdateBefore")
  public boolean getGenerateUpdateBefore() {
    return generateUpdateBefore;
  }

  @JsonGetter("appendOnly")
  public boolean getAppendOnly() {
    return appendOnly;
  }

  @JsonGetter("inputType")
  public RowType getInputType() {
    return inputType;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }
}
