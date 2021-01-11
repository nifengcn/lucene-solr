/*
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
package org.apache.lucene.index;


import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.WindowsFS;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.Ignore;
import org.junit.Test;
import java.nio.file.Paths;

public class TestCreateIdx extends LuceneTestCase {

  private static final FieldType storedTextType = new FieldType(TextField.TYPE_NOT_STORED);

  public void testDocCount() throws IOException, InterruptedException {
    String prefix = "/Users/nifeng/dev/lucene-solr/lucene/core/build/tmp/tests-tmp/persis/";
    RandomizedContext ctx = RandomizedContext.current();
    String path_str = prefix + ctx.getRunnerSeedAsString();
    Path path = Paths.get(path_str);
    //Path path = createTempDir();
    Files.createDirectory(path);
    NIOFSDirectory dir = new NIOFSDirectory(path);
    //Directory dir = newDirectory();
    //Random r = random();
    //Directory dir = wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), rarely(r), true);
    //for (StackTraceElement item:Thread.currentThread().getStackTrace()) {
    //  System.out.println(item.getFileName() + ":" + item.getLineNumber() + " " + item.getMethodName());
    //}

    System.out.println("begin" + Thread.currentThread().getStackTrace()[1].getLineNumber());
    System.out.println(path.toString());
    System.out.println(Arrays.toString(dir.listAll()));
    System.out.println("end" + Thread.currentThread().getStackTrace()[1].getLineNumber());

    IndexWriter writer = null;
    IndexReader reader = null;
    int i;

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    // add 100 documents
    for (i = 0; i < 100; i++) {
      addDocWithIndex(writer, i);
      if (random().nextBoolean()) {
        writer.commit();
      }
    }
    IndexWriter.DocStats docStats = writer.getDocStats();
    assertEquals(100, docStats.maxDoc);
    assertEquals(100, docStats.numDocs);
    writer.close();

    // delete 40 documents
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergePolicy(new FilterMergePolicy(NoMergePolicy.INSTANCE) {
              @Override
              public boolean keepFullyDeletedSegment(IOSupplier<CodecReader>
                                                             readerIOSupplier) {
                return true;
              }
            }));

    for (i = 0; i < 40; i++) {
      writer.deleteDocuments(new Term("id", "" + i));
      if (random().nextBoolean()) {
        writer.commit();
      }
    }
    writer.flush();
    docStats = writer.getDocStats();
    assertEquals(100, docStats.maxDoc);
    assertEquals(60, docStats.numDocs);
    writer.close();

    /*
    reader = DirectoryReader.open(dir);
    assertEquals(60, reader.numDocs());
    reader.close();

    // merge the index down and check that the new doc count is correct
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    assertEquals(60, writer.getDocStats().numDocs);
    writer.forceMerge(1);
    docStats = writer.getDocStats();
    assertEquals(60, docStats.maxDoc);
    assertEquals(60, docStats.numDocs);
    writer.close();

    // check that the index reader gives the same numbers.
    reader = DirectoryReader.open(dir);
    assertEquals(60, reader.maxDoc());
    assertEquals(60, reader.numDocs());
    reader.close();

    // make sure opening a new index for create over
    // this existing one works correctly:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.CREATE));
    docStats = writer.getDocStats();
    assertEquals(0, docStats.maxDoc);
    assertEquals(0, docStats.numDocs);
    writer.close();

     */

    System.out.println("begin");
    System.out.println(Arrays.toString(dir.listAll()));
    System.out.println("end");
    Thread.sleep(3000);

    dir.close();
  }

  static void addDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    writer.addDocument(doc);
  }

  static void addDocWithIndex(IndexWriter writer, int index) throws IOException {
    Document doc = new Document();
    doc.add(newField("content", "aaa " + index, storedTextType));
    doc.add(newField("id", "" + index, storedTextType));
    writer.addDocument(doc);
  }

  // TODO: we have the logic in MDW to do this check, and it's better, because it knows about files it tried
  // to delete but couldn't: we should replace this!!!!
  public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
    String[] startFiles = dir.listAll();
    new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random()))).rollback();
    String[] endFiles = dir.listAll();

    Arrays.sort(startFiles);
    Arrays.sort(endFiles);

    if (!Arrays.equals(startFiles, endFiles)) {
      fail(message + ": before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
    }
  }

  static String arrayToString(String[] l) {
    String s = "";
    for (int i = 0; i < l.length; i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }

  /**
   * Returns how many unique segment names are in the directory.
   */
  private static int getSegmentCount(Directory dir) throws IOException {
    Set<String> segments = new HashSet<>();
    for (String file : dir.listAll()) {
      segments.add(IndexFileNames.parseSegmentName(file));
    }

    return segments.size();
  }
}

