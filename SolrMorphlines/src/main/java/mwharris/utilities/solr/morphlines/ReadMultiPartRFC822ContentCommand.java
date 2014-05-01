/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mwharris.utilities.solr.morphlines;

import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

/**
 *
 * @author mharris
 */
public final class ReadMultiPartRFC822ContentCommand implements CommandBuilder {

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("readMultiPartRFC822Content");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ReadMultiPartRFC822Content(this, config, parent, child, context);
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class ReadMultiPartRFC822Content extends AbstractParser {

        private final String outputField;

        public ReadMultiPartRFC822Content(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            this.outputField = getConfigs().getString(config, "outputField", "");
            if (outputField.isEmpty()) {
                throw new MorphlineCompilationException("One output field must be specified", config);
            }
            validateArguments();
        }

        @Override
        protected boolean doProcess(Record inputRecord, InputStream input) throws IOException {

            Record template = inputRecord.copy();
            List attachments = inputRecord.get(Fields.ATTACHMENT_BODY);
            if (attachments.isEmpty()) {
                throw new IOException("The attachment is Empty, please assign HBase output column to _attachment_body");
                //return false;
            }
            Record outputRecord = template.copy();

            ParseContext context = new ParseContext();
            Detector detector = new DefaultDetector();
            Parser parser = new AutoDetectParser(detector);
            context.set(Parser.class, parser);

            Metadata metadata = new Metadata();
            BodyContentHandler ch;
            ch = new BodyContentHandler();
            try {
                Parser p = parser;
                ch = new BodyContentHandler();
                p.parse(input, ch, metadata, context);
            } catch (Exception e) {
                throw new IOException("Parsing failed");
            } finally {
                input.close();
                System.out.flush();
            }
            String replaced = ch.toString().replaceAll("(Sender)(:)(\\s+)([\\w-+]+(?:\\.[\\w-+]+)*@(?:[\\w-]+\\.)+[a-zA-Z]{2,7})([\\n\\r\\s]+)", "");
            replaced = replaced.replaceAll("(Subject)(:).*?([\\n\\r]+)", "");
            replaced = replaced.replaceAll("(Message-Id:).*?([\\n\\r]+)", "");
            replaced = replaced.replaceAll("(To)(:)(\\s+)([\\w-+]+(?:\\.[\\w-+]+)*@(?:[\\w-]+\\.)+[a-zA-Z]{2,7})([\\n\\r]+)", "");
            replaced = replaced.replaceAll("(Cc)(:)(\\s+)([\\w-+]+(?:\\.[\\w-+]+)*@(?:[\\w-]+\\.)+[a-zA-Z]{2,7})([\\n\\r]+)", "");
            replaced = replaced.replaceAll("(Bcc)(:)(\\s+)([\\w-+]+(?:\\.[\\w-+]+)*@(?:[\\w-]+\\.)+[a-zA-Z]{2,7})([\\n\\r]+)", "");
            replaced = replaced.replaceAll("([\\n\\r]+)", "\n");
            outputRecord.put(this.outputField, replaced);
            //outputRecord.put(Fields.MESSAGE, replaced);
            if (!getChild().process(outputRecord)) {
                return false;
            }
            return true;
            // Need to add the ability to traverse documents via Tika.
        }
    }
}