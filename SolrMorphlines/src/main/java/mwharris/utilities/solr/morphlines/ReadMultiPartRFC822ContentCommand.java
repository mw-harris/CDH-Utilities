/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mwharris.utilities.solr.morphlines;

import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mail.RFC822Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.xml.sax.SAXException;

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
            this.outputField = getConfigs().getString(config, "outputField","");
            if (outputField.isEmpty()) {
                throw new MorphlineCompilationException("One output field must be specified",config);
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

            Metadata metadata = new Metadata();
            BodyContentHandler ch = new BodyContentHandler();
            Parser parser = new RFC822Parser();
            String mimeType = new Tika().detect(input);
            

            if (!(mimeType.equalsIgnoreCase("message/rfc822"))) {
                throw new IOException("Not an RFC822 document");
                //return false;
            }

            metadata.set(Metadata.CONTENT_TYPE, mimeType);
            try {
                parser.parse(input, ch, metadata, new ParseContext());
            } catch (SAXException ex) {
                Logger.getLogger(ReadMultiPartRFC822ContentCommand.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TikaException ex) {
                Logger.getLogger(ReadMultiPartRFC822ContentCommand.class.getName()).log(Level.SEVERE, null, ex);
            }
            input.close();

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
        }
    }
}