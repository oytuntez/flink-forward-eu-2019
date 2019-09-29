package proofreaders.common;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public enum EventType implements Serializable {

    // MW Events
    VENDOR_INVITED("VendorInvited"),
    VENDORS_INVITED("VendorsInvited"),
    PROJECT_COMPLETED("ProjectCompleted"),
    VENDOR_ENTERED_PROJECT("VendorEnteredProject"),
    VENDOR_EARNED_PO("VendorEarnedPO"),
    TOO_FREQUENT_TRANSLATION("TooFrequentTranslation"),
    PROJECT_DOCUMENT_UPDATED("ProjectDocumentUpdated"),
    PROJECT_DOCUMENTS_UPDATED("ProjectDocumentsUpdated"),
    PROJECT_TARGET_LANGUAGE_UPDATED("ProjectTargetLanguageUpdated"),
    PROJECT_TARGET_LANGUAGES_UPDATED("ProjectTargetLanguagesUpdated"),
    PROOFREADER_TAKEN_PROJECT("ProofreaderTakenProject"),
    SAVE_FINISHED("SAVE_FINISHED"),
    PROJECT_CREATED("ProjectCreated"),
    PROJECT_RECREATED("ProjectRecreated"),
    INVITE_PROOFREADER("InviteProofreader"),
    PROOFREADER_CANDIDATE_DETECTED("ProofreaderCandidateDetected"),
    PROOFREADER_FEEDBACK("ProofreaderFeedback"),
    MACHINE_TRANSLATION_DETECTED("MachineTranslationDetected"),
    GRAMMATICAL_ERRORS_DETECTED("GrammaticalErrorsDetected"),
    MACHINE_TRANSLATION_TREND_DETECTED("MachineTranslationTrendDetected"),
    GRAMMATICAL_ERROR_TREND_DETECTED("GrammaticalErrorTrendDetected"),
    UNRESPONSIVE_VENDOR_DETECTED("UnresponsiveVendorDetected"),
    USER_LOGGED_IN("UserLoggedIn"),

    // Mautic Events
    CLIENT_RETAINED("ClientRetained"),
    CLIENT_ATTRITED("ClientAttrited"),
    EMAIL_OPENED("EmailOpened"),
    EMAIL_SENT("EmailSent"),
    EMAIL_REPLIED("EmailReplied"),
    PAGE_VISITED("PageVisited"),
    PAGE_DISPLAYED("PageDisplayed"),
    FORM_SUBMITTED("FormSubmitted"),

    // Other
    UNKNOWN_EVENT_TYPE("UNKNOWN_EVENT_TYPE"),
    DUMMY("Dummy");

    private String value;

    private static final Map<String, EventType> lookup = new HashMap<>();

    static {
        for (EventType d : EventType.values()) {
            lookup.put(d.getName(), d);
        }
    }

    EventType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getName() {
        return value;
    }

    public static EventType get(String name) {
        return lookup.getOrDefault(name, UNKNOWN_EVENT_TYPE);
    }
}
