package topica.cdp.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.ValidationResult.Builder;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor;
import org.apache.nifi.processors.mongodb.ObjectIdSerializer;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

@Tags({"mongodb", "read", "get", "cdp"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Get mongo incremental(beta version)")
public class CDPGetMongoIncrementalV1 extends AbstractMongoProcessor {
    public static final Validator DOCUMENT_VALIDATOR = (subject, value, context) -> {
        Builder builder = new Builder();
        builder.subject(subject).input(value);
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return builder.valid(true).explanation("Contains Expression Language").build();
        } else {
            String reason = null;

            try {
                Document.parse(value);
            } catch (RuntimeException var7) {
                reason = var7.getLocalizedMessage();
            }

            return builder.explanation(reason).valid(reason == null).build();
        }
    };
    static final Relationship REL_SUCCESS = (new org.apache.nifi.processor.Relationship.Builder()).name("success").description("All files are routed to success").build();
    static final PropertyDescriptor QUERY;

    //TruongLX
    static final PropertyDescriptor QUERY_INCREMENTAL;
    static final PropertyDescriptor VALUE_DEFAULT_INCREMENTAL;
    static final PropertyDescriptor RANGE_INCREMENTAL;
    //static final PropertyDescriptor FIELD_NAME_FILE;
    static final PropertyDescriptor RESET_QUERY;

    static final PropertyDescriptor PROJECTION;
    static final PropertyDescriptor SORT;
    static final PropertyDescriptor LIMIT;
    static final PropertyDescriptor BATCH_SIZE;
    static final PropertyDescriptor RESULTS_PER_FLOWFILE;

    static final String JSON_TYPE_EXTENDED = "Extended";
    static final String JSON_TYPE_STANDARD = "Standard";
    static final AllowableValue JSON_EXTENDED;
    static final AllowableValue JSON_STANDARD;
    static final PropertyDescriptor JSON_TYPE;
    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;
    private ObjectMapper mapper;

    static final DateFormat FORMAT_DATE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static final String date = "2015-01-01T00:00:00.000Z";
    static final ConcurrentHashMap<String, String> dateMap = new ConcurrentHashMap<>();
    static final Object _lock = new Object();

    /*  static String getDate() {
          synchronized (_lock) {
              return date;
          }
      }

      static void setDate(String date1) {
          synchronized (_lock) {
              date = date1;
          }
      }
  */
    public CDPGetMongoIncrementalV1() {
    }

    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    private String buildBatch(List<Document> documents, String jsonTypeSetting) throws IOException {
        StringBuilder builder = new StringBuilder();

        for (int index = 0; index < documents.size(); ++index) {
            Document document = (Document) documents.get(index);
            String asJson;
            if (jsonTypeSetting.equals("Standard")) {
                asJson = this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
            } else {
                asJson = document.toJson(new JsonWriterSettings(true));
            }

            builder.append(asJson).append(documents.size() > 1 && index + 1 < documents.size() ? ", " : "");
        }

        return "[" + builder.toString() + "]";
    }

    private void configureMapper(String setting) {
        this.mapper = new ObjectMapper();
        if (setting.equals("Standard")) {
            this.mapper.registerModule(ObjectIdSerializer.getModule());
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            this.mapper.setDateFormat(df);

        }

    }

    private void writeBatch(final String payload, ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                out.write(payload.getBytes("UTF-8"));
            }
        });
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.getProvenanceReporter().receive(flowFile, this.getURI(context));
        session.transfer(flowFile, REL_SUCCESS);
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = this.getLogger();
        Document query = context.getProperty(QUERY).isSet() ? Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions().getValue()) : null;
        Date newDate = null;
        String key = context.getProperty(DATABASE_NAME).getValue() + context.getProperty(COLLECTION_NAME).getValue();
        String sDate = dateMap.get(key);
        //TruongLX:Update incremental
        try {
            if (context.getProperty(QUERY_INCREMENTAL).isSet()) {

                if (context.getProperty(VALUE_DEFAULT_INCREMENTAL).isSet()) {
                    if (sDate == null || FORMAT_DATE.parse(context.getProperty(VALUE_DEFAULT_INCREMENTAL).getValue()).compareTo(FORMAT_DATE.parse(sDate)) > 0) {
                        sDate = context.getProperty(VALUE_DEFAULT_INCREMENTAL).getValue();
                    }
                }

                //Khong incrementall tu trang thai cu nua
                if (context.getProperty(RESET_QUERY).isSet() && context.getProperty(RESET_QUERY).asBoolean()) {
                    dateMap.put(key, context.getProperty(VALUE_DEFAULT_INCREMENTAL).getValue());
                }

                Calendar cal = Calendar.getInstance();
                cal.setTime(FORMAT_DATE.parse(sDate));
                cal.add(Calendar.DATE, context.getProperty(RANGE_INCREMENTAL).asInteger());
                newDate = cal.getTime();
                if (query != null) {
                    query.append(context.getProperty(QUERY_INCREMENTAL).getValue(), new Document("$gte", FORMAT_DATE.parse(sDate)).append("$lt", newDate));

                } else {
                    query = new Document(context.getProperty(QUERY_INCREMENTAL).getValue(), new Document("$gte", FORMAT_DATE.parse(sDate)).append("$lt", newDate));
                }
            }
            //this.getLogger().error(query.toJson());
            // this.getLogger().error(FORMAT_DATE.format(newDate).toString());
        } catch (ParseException e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        Document projection = context.getProperty(PROJECTION).isSet() ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions().getValue()) : null;
        Document sort = context.getProperty(SORT).isSet() ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions().getValue()) : null;
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        this.configureMapper(jsonTypeSetting);
        MongoCollection collection = this.getCollection(context);

        try {
            FindIterable<Document> it = query != null ? collection.find(query) : collection.find();
            if (projection != null) {
                it.projection(projection);
            }

            if (sort != null) {
                it.sort(sort);
            }

            if (context.getProperty(LIMIT).isSet()) {
                it.limit(context.getProperty(LIMIT).evaluateAttributeExpressions().asInteger());
            }

            if (context.getProperty(BATCH_SIZE).isSet()) {
                it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
            }

            final MongoCursor<Document> cursor = it.iterator();
            ComponentLog log = this.getLogger();

            try {
                FlowFile flowFile = null;
                if (!context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                    while (cursor.hasNext()) {
                        flowFile = session.create();
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            public void process(OutputStream out) throws IOException {
                                String json;
                                if (jsonTypeSetting.equals("Standard")) {
                                    json = CDPGetMongoIncrementalV1.this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cursor.next());
                                } else {
                                    json = ((Document) cursor.next()).toJson();
                                }

                                IOUtils.write(json, out);
                            }
                        });
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                        session.getProvenanceReporter().receive(flowFile, this.getURI(context));
                        //this.getLogger().error("Size: " + flowFile.getSize());
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                } else {
                    int ceiling = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
                    ArrayList batch = new ArrayList();

                    while (cursor.hasNext()) {
                        batch.add(cursor.next());
                        if (batch.size() == ceiling) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Writing batch...");
                                }

                                String payload = this.buildBatch(batch, jsonTypeSetting);
                                this.writeBatch(payload, context, session);
                                batch = new ArrayList();
                            } catch (IOException var22) {
                                this.getLogger().error("Error building batch", var22);
                            }
                        }
                    }

                    if (batch.size() > 0) {
                        try {
                            this.writeBatch(this.buildBatch(batch, jsonTypeSetting), context, session);

                        } catch (IOException var21) {
                            this.getLogger().error("Error sending remainder of batch", var21);
                        }
                    }
                }

                logger.error("(Not Error!)Date: " + sDate.toString());
               /* session.putAttribute(
                        flowFile,
                        context.getProperty(FIELD_NAME_FILE).isSet() ? context.getProperty(FIELD_NAME_FILE).getValue() : "created_at",
                        sDate);*/
                session.commit();
                //TruongLX: Update timestamp
                if (newDate != null && newDate.compareTo(Calendar.getInstance().getTime()) <= 0)
                    dateMap.put(key, FORMAT_DATE.format(newDate).toString());
            } finally {
                cursor.close();
            }
        } catch (RuntimeException var24) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[]{query, var24}, var24);
        }

    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    static {
        QUERY = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Query").description("The selection criteria; must be a valid MongoDB Extended JSON format; if omitted the entire collection will be queried").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();

        //TruongLX
        QUERY_INCREMENTAL = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Field query incremental").description("File query incremental(datetime)").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        VALUE_DEFAULT_INCREMENTAL = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Value default incremental").description("Value start(datetime:yyyy-MM-ddTHH:mm:ss.SSSZ").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue(date).build();
        RANGE_INCREMENTAL = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Range").description("Number of date").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("1").build();
        // FIELD_NAME_FILE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Field File Name").description("File name(created_at or updated_at)").required(false).expressionLanguageSupported(false).defaultValue("created_at").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        RESET_QUERY = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Reset query").description("Reset date increment -> value").required(false).expressionLanguageSupported(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("false").build();

        PROJECTION = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Projection").description("The fields to be returned from the documents in the result set; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        SORT = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Sort").description("The fields by which to sort; must be a valid BSON document").required(false).expressionLanguageSupported(true).addValidator(DOCUMENT_VALIDATOR).build();
        LIMIT = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Limit").description("The maximum number of elements to return").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        BATCH_SIZE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Batch Size").description("The number of elements returned from the server in one batch").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        RESULTS_PER_FLOWFILE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("results-per-flowfile").displayName("Results Per FlowFile").description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.").required(false).expressionLanguageSupported(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        JSON_EXTENDED = new AllowableValue("Extended", "Extended JSON", "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
        JSON_STANDARD = new AllowableValue("Standard", "Standard JSON", "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");
        JSON_TYPE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).allowableValues(new AllowableValue[]{JSON_EXTENDED, JSON_STANDARD}).defaultValue("Extended").displayName("JSON Type").name("json-type").description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting  controls whether to use extended JSON or provide a clean view that conforms to standard JSON.").expressionLanguageSupported(false).required(true).build();
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList();
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(DATABASE_NAME);
        _propertyDescriptors.add(COLLECTION_NAME);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(PROJECTION);
        _propertyDescriptors.add(SORT);
        _propertyDescriptors.add(LIMIT);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        _propertyDescriptors.add(QUERY_INCREMENTAL);
        _propertyDescriptors.add(VALUE_DEFAULT_INCREMENTAL);
        _propertyDescriptors.add(RANGE_INCREMENTAL);
        // _propertyDescriptors.add(FIELD_NAME_FILE);
        _propertyDescriptors.add(RESET_QUERY);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);

        FORMAT_DATE.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
}
