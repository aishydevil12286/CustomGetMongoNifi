/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topica.cdp.nifi.processors.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This is meant to be for general use for converting different lists of types
 * into their appropriate type and then to output that info in their proper
 * types within JSON.
 *
 * @author jeremytaylor
 */
@EventDriven
@SideEffectFree
@Tags({"json", "convert", "attribute", "attributes", "types", "String", "Integer", "Double", "Date", "array", "sum of array"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Takes in a String list, Integer list, Double list, and Date list of attributes and converts the attributes accordingly in JSON format based on what type-list the item is placed in.  Boolean values should be listed in String list and will automatically be converted to Boolean. Performs sums on Integer or Double attributes that are arrays.")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
public class BetterAttributesToJSON extends AbstractProcessor {

    private static final String AT_LIST_SEPARATOR = ",";
    private static final String APPLICATION_JSON = "application/json";
    private static final String MONGO_TIME_ZONE = "GMT-0";
    private static final String MONGO_DATE_TEMPLATE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     * to provide a list of attributes to serialize out as strings in JSON
     */
    public static final PropertyDescriptor STRING_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("String Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as String values. If this value "
                    + "is left empty then all existing Attributes will be included. This list of attributes is "
                    + "case sensitive. If an attribute specified in the list is not found it will be be emitted "
                    + "to the resulting JSON with an empty string or NULL value. "
                    + "Boolean values should be placed in this list and will automatically be converted to Boolean when placed here.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * to provide a list of attributes to serialize out as booleans in JSON
     */
    public static final PropertyDescriptor BOOLEAN_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Boolean Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as boolean values. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * to provide a list of attributes to serialize out as integers in JSON
     */
    public static final PropertyDescriptor INT_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Integer Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as Integer values. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * to provide a list of attributes to serialize out as doubles in JSON
     */
    public static final PropertyDescriptor DOUBLE_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Double Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as Double values. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * to provide a list of attributes to serialize out as ISODates for mongo in
     * JSON
     */
    public static final PropertyDescriptor EPOCH_TO_DATES_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Epoch Dates Attributes List (Longs)")
            .description("Comma separated list of attributes to be included in the resulting JSON that will be converted from Long Epoch values to Date values. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * to provide list of attributes that consist of an array of Double values
     * that will be converted to the sum of those array values
     */
    public static final PropertyDescriptor DOUBLE_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Double Array To Sum Value Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as the sum of its own Double array values. "
                    + "Thus, each attribute that has an array of Doubles that is listed here will return a sum. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * to provide list of attributes that consist of an array of Integer values
     * that will be converted to the sum of those array values
     */
    public static final PropertyDescriptor INT_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Integer Array To Sum Value Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON as the sum of its own Integer array values. "
                    + "Thus, each attribute that has an array of Integers that is listed here will return a sum. If this value "
                    + "is left empty then this is ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted raw security marking attribute to multiple attributes").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert raw security marking attribute to multiple attributes").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(STRING_ATTRIBUTES_LIST);
        _properties.add(BOOLEAN_ATTRIBUTES_LIST);
        _properties.add(INT_ATTRIBUTES_LIST);
        _properties.add(DOUBLE_ATTRIBUTES_LIST);
        _properties.add(EPOCH_TO_DATES_ATTRIBUTES_LIST);
        _properties.add(DOUBLE_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST);
        _properties.add(INT_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is
     * emitted from this process.
     *
     * @param ff
     * @param atrListForStringValues
     * @param atrListForBooleanValues
     * @param atrListForIntValues
     * @param atrListForDoubleValues
     * @param atrListForLongEpochToGoToDateValues
     * @param atrListForDoubleArraysToGoToDoubleSumValues
     * @param atrListForIntegerArraysToGoToIntegerSumValues
     *
     * @return Map of values that are feed to a Jackson ObjectMapper
     * @throws java.io.IOException
     */
    protected Map<String, Object> buildAttributesMapAndBringInFlowAttrs(
            FlowFile ff,
            String atrListForStringValues,
            String atrListForBooleanValues,
            String atrListForIntValues,
            String atrListForDoubleValues,
            String atrListForLongEpochToGoToDateValues,
            String atrListForDoubleArraysToGoToDoubleSumValues,
            String atrListForIntegerArraysToGoToIntegerSumValues) throws IOException {
        Map<String, Object> atsToWrite = new HashMap<>();

        //handle all the string values
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrListForStringValues)) {
            String[] ats = StringUtils.split(atrListForStringValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        atsToWrite.put(cleanStr, "");
                    }
                }
            }

        } else {
            atsToWrite.putAll(ff.getAttributes());
        }
        //handle all boolean values
        if (StringUtils.isNotBlank(atrListForBooleanValues)) {
            String[] ats = StringUtils.split(atrListForBooleanValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, Boolean.parseBoolean(val));
                    } else {
                        //for boolean, place false and not null when null value -- special case
                        atsToWrite.put(cleanStr, false);
                    }
                }
            }
        }
        //handle all int values
        if (StringUtils.isNotBlank(atrListForIntValues)) {
            String[] ats = StringUtils.split(atrListForIntValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, Integer.parseInt(val));
                    } else {
                        atsToWrite.put(cleanStr, null);
                    }
                }
            }
        }
        //handle all double values
        if (StringUtils.isNotBlank(atrListForDoubleValues)) {
            String[] ats = StringUtils.split(atrListForDoubleValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, Double.parseDouble(val));
                    } else {
                        atsToWrite.put(cleanStr, null);
                    }
                }
            }
        }
        //handle all date values
        if (StringUtils.isNotBlank(atrListForLongEpochToGoToDateValues)) {
            String[] ats = StringUtils.split(atrListForLongEpochToGoToDateValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        long epochTime = Long.parseLong(val);
                        GregorianCalendar gcal = new GregorianCalendar();
                        gcal.setTimeZone(TimeZone.getTimeZone(MONGO_TIME_ZONE));
                        gcal.setTimeInMillis(epochTime);
                        SimpleDateFormat sdf = new SimpleDateFormat(MONGO_DATE_TEMPLATE);
                        String mongoDate = sdf.format(gcal.getTime());
                        //to Date
                        Map<String, String> isoDate = new HashMap<>();
                        isoDate.put("$date", mongoDate);
                        atsToWrite.put(cleanStr, isoDate);
                    } else {
                        atsToWrite.put(cleanStr, null);
                    }
                }
            }
        }
        //handle all double Arrays to give sums of array
        if (StringUtils.isNotBlank(atrListForDoubleArraysToGoToDoubleSumValues)) {
            String[] ats = StringUtils.split(atrListForDoubleArraysToGoToDoubleSumValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String jsonArray = ff.getAttribute(cleanStr);
                    if (jsonArray != null) {
                        ObjectMapper objMapper = new ObjectMapper();
                        Double[] doubleValues = objMapper.readValue(jsonArray, Double[].class);
                        Double sum = 0.0D;
                        for (Double val : doubleValues) {
                            sum += val;
                        }
                        atsToWrite.put(cleanStr, sum);
                    } else {
                        atsToWrite.put(cleanStr, null);
                    }
                }
            }
        }
        //handle all integer Arrays to give sums of array
        if (StringUtils.isNotBlank(atrListForDoubleArraysToGoToDoubleSumValues)) {
            String[] ats = StringUtils.split(atrListForIntegerArraysToGoToIntegerSumValues, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String jsonArray = ff.getAttribute(cleanStr);
                    if (jsonArray != null) {
                        ObjectMapper objMapper = new ObjectMapper();
                        Integer[] intValues = objMapper.readValue(jsonArray, Integer[].class);
                        Integer sum = 0;
                        for (Integer val : intValues) {
                            sum += val;
                        }
                        atsToWrite.put(cleanStr, sum);
                    } else {
                        atsToWrite.put(cleanStr, null);
                    }
                }
            }
        }
        return atsToWrite;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        try {
            final Map<String, Object> attrsToUpdate = buildAttributesMapAndBringInFlowAttrs(original,
                    context.getProperty(STRING_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(BOOLEAN_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(INT_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(DOUBLE_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(EPOCH_TO_DATES_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(DOUBLE_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST).getValue(),
                    context.getProperty(INT_ARRAY_TO_SUM_VALUE_ATTRIBUTES_LIST).getValue());

            FlowFile conFlowfile = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        ObjectMapper objMapper = new ObjectMapper();
                        outputStream.write(objMapper.writeValueAsBytes(attrsToUpdate));
                    }
                }
            });
            conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            session.transfer(conFlowfile, REL_SUCCESS);

        } catch (IOException ioe) {
            logger.error(ioe.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }

}
