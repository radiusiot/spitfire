/*
 * Apache Zeppelin R Interpreter
 *
 * Copyright (c) 2015 Datalayer (http://datalayer.io)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *This program is distributed in the hope that it will be useful,
 *but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.apache.zeppelin.spark;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;
import org.rosuda.REngine.Rserve.StartRserve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * R and SparkR interpreter for Apache Zeppelin.
 */
public class SparkRInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(SparkRInterpreter.class);
  private static RConnection con;
  private boolean firstStart = true;

  static {
    Interpreter.register("r", "spark", SparkRInterpreter.class.getName());
  }

  public SparkRInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    try {
      StartRserve.checkLocalRserve();
      con = new RConnection();
      con.voidEval("library('knitr')");
      con.voidEval("getFunctionNames <- function() {\n" +
          "    loaded <- (.packages())\n" +
          "    loaded <- paste(\"package:\", loaded, sep =\"\")\n" +
          "    return(sort(unlist(lapply(loaded, lsf.str))))\n" +
          "}");
      logger.info("Connected to an Rserve instance");
    } catch (RserveException e) {
      logger.error("No Rserve instance available!", e);
    }
  }

  @Override
  public void close() {
    try {
      con.shutdown();
    } catch (RserveException e) {
      e.getMessage();
    }
  }

  @Override
  public InterpreterResult interpret(String lines, InterpreterContext contextInterpreter) {

    if (!con.isConnected()){
      return new InterpreterResult(InterpreterResult.Code.ERROR, "No connection to Rserve");
    }

    // WORKAROUND: Rserve fails the first time (StartRserve may be involved...).
    if (firstStart) {
      firstStart = false;
      interpret("print(\"First start\")", contextInterpreter);
    }

    logger.info("Run R command '" + lines + "'");

    BufferedWriter writer = null;
    try {
      File in = File.createTempFile("forKnitR-" +
          contextInterpreter.getParagraphId(), ".Rmd");
      String inPath = in.getAbsolutePath().substring(0, in.getAbsolutePath().length() - 4);
      File out = new File(inPath + ".html");

      writer = new BufferedWriter(new FileWriter(in));
      writer.write("\n```{r comment=NA, echo=FALSE}\n" + lines + "\n```");
      writer.close();

      String rcmd = "knit2html('" + in.getAbsolutePath() + "', output = '"
          + out.getAbsolutePath() + "')" + "\n";

      con.voidEval(rcmd);

      String html = new String(Files.readAllBytes(out.toPath()));

      // Only keep the bare results.
      String htmlOut = html.substring(html.indexOf("<body>") + 7, html.indexOf("</body>") - 1)
          .replaceAll("<code>", "").replaceAll("</code>", "")
          .replaceAll("\n\n", "")
          .replaceAll("\n", "<br>")
          .replaceAll("<pre>", "<p class='text'>").replaceAll("</pre>", "</p>");

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, "%html\n" + htmlOut);
    } catch (RserveException e) {
      logger.error("Exception while connecting to Rserve", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } catch (java.io.IOException e) {
      logger.error("Exception while connecting to Rserve", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    } finally {
      try {
        writer.close();
      } catch (Exception e) {
        // Do nothing...
      }
    }
  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.NONE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        SparkRInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    List<String> list = new ArrayList<String>();
    String[] funcList;
    String[] varList;
    try {
      varList = con.eval("ls()").asStrings();
      funcList = con.eval("getFunctionNames()").asStrings();
      String before = buf.substring(0, cursor - 1);
      List<String> listFunc = new ArrayList<String>(Arrays.asList(funcList));
      List<String> listVar = new ArrayList<String>(Arrays.asList(varList));
      listVar.remove("getFunctionNames");
      if (before.endsWith("\n") || before.endsWith(" ")) {
        list.addAll(listVar);
      } else {
        String[] tokenize = before.replaceAll("\n", " ").split(" ");
        String lastWord = tokenize[tokenize.length - 1];
        for (String s: listVar) {
          if (s.startsWith(lastWord)) list.add(s);
        }
        for (String s: listFunc) {
          if (s.startsWith(lastWord)) list.add(s);
        }
      }

    } catch (RserveException e) {
      logger.warn(e.getMessage());
    } catch (REXPMismatchException e) {
      logger.warn(e.getMessage());
    }

    return list;
  }

}
