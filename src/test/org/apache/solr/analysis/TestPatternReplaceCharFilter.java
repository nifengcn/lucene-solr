/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * 
 * @version $Id$
 *
 */
public class TestPatternReplaceCharFilter extends BaseTokenTestCase {
  
  //           1111
  // 01234567890123
  // this is test.
  public void testNothingChange() throws IOException {
    final String BLOCK = "this is test.";
    PatternReplaceCharFilterFactory factory = new PatternReplaceCharFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("pattern", "(aa)\\s+(bb)\\s+(cc)");
    args.put("replacement", "$1$2$3");
    factory.init(args);
    CharStream cs = factory.create(
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "test." },
        new int[] { 0, 5, 8 },
        new int[] { 4, 7, 13 },
        new int[] { 1, 1, 1 });
  }
  
  // 012345678
  // aa bb cc
  public void testReplaceByEmpty() throws IOException {
    final String BLOCK = "aa bb cc";
    PatternReplaceCharFilterFactory factory = new PatternReplaceCharFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("pattern", "(aa)\\s+(bb)\\s+(cc)");
    factory.init(args);
    CharStream cs = factory.create(
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertFalse(ts.incrementToken());
  }
  
  // 012345678
  // aa bb cc
  // aa#bb#cc
  public void test1block1matchSameLength() throws IOException {
    final String BLOCK = "aa bb cc";
    PatternReplaceCharFilterFactory factory = new PatternReplaceCharFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("pattern", "(aa)\\s+(bb)\\s+(cc)");
    args.put("replacement", "$1#$2#$3");
    factory.init(args);
    CharStream cs = factory.create(
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa#bb#cc" },
        new int[] { 0 },
        new int[] { 8 },
        new int[] { 1 });
  }

  //           11111
  // 012345678901234
  // aa bb cc dd
  // aa##bb###cc dd
  public void test1block1matchLonger() throws IOException {
    final String BLOCK = "aa bb cc dd";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1##$2###$3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa##bb###cc", "dd" },
        new int[] { 0, 9 },
        new int[] { 8, 11 },
        new int[] { 1, 1 });
  }

  // 01234567
  //  a  a
  //  aa  aa
  public void test1block2matchLonger() throws IOException {
    final String BLOCK = " a  a";
    CharStream cs = new PatternReplaceCharFilter( "a", "aa",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa", "aa" },
        new int[] { 1, 4 },
        new int[] { 2, 5 },
        new int[] { 1, 1 });
  }

  //           11111
  // 012345678901234
  // aa  bb   cc dd
  // aa#bb dd
  public void test1block1matchShorter() throws IOException {
    final String BLOCK = "aa  bb   cc dd";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1#$2",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa#bb", "dd" },
        new int[] { 0, 12 },
        new int[] { 11, 14 },
        new int[] { 1, 1 });
  }

  //           111111111122222222223333
  // 0123456789012345678901234567890123
  //   aa bb cc --- aa bb aa   bb   cc
  //   aa  bb  cc --- aa bb aa  bb  cc
  public void test1blockMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa   bb   cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1  $2  $3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa", "bb", "cc", "---", "aa", "bb", "aa", "bb", "cc" },
        new int[] { 2, 6, 9, 11, 15, 18, 21, 25, 29 },
        new int[] { 4, 8, 10, 14, 17, 20, 23, 27, 33 },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1 });
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //   aa bb cc --- aa bb aa. bb aa   bb cc
  //   aa##bb cc --- aa##bb aa. bb aa##bb cc
  public void test2blocksMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa. bb aa   bb cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)", "$1##$2", ".",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa##bb", "cc", "---", "aa##bb", "aa.", "bb", "aa##bb", "cc" },
        new int[] { 2, 8, 11, 15, 21, 25, 28, 36 },
        new int[] { 7, 10, 14, 20, 24, 27, 35, 38 },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1 });
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //  a bb - ccc . --- bb a . ccc ccc bb
  //  aa b - c . --- b aa . c c b
  public void testChain() throws IOException {
    final String BLOCK = " a bb - ccc . --- bb a . ccc ccc bb";
    CharStream cs = new PatternReplaceCharFilter( "a", "aa", ".",
        CharReader.get( new StringReader( BLOCK ) ) );
    cs = new PatternReplaceCharFilter( "bb", "b", ".", cs );
    cs = new PatternReplaceCharFilter( "ccc", "c", ".", cs );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokenStreamContents(ts,
        new String[] { "aa", "b", "-", "c", ".", "---", "b", "aa", ".", "c", "c", "b" },
        new int[] { 1, 3, 6, 8, 12, 14, 18, 21, 23, 25, 29, 33 },
        new int[] { 2, 5, 7, 11, 13, 17, 20, 22, 24, 28, 32, 35 },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });
  }
}
