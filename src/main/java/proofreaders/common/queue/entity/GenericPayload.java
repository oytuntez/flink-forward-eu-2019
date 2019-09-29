package proofreaders.common.queue.entity;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import proofreaders.common.LanguagePair;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenericPayload implements Serializable {

    private Long projectId;
    private Long quoteId;
    private Long vendorId;
    private Long fileId;
    private String message;
    private Long documentId;
    private Long userId;
    private Long user_id;
    private Long clientId;
    private String targetLanguage;
    private String sourceLanguage;
    private List subEvents;
    private Double score;
    private String email;
    private String editReason;
    private Boolean hasEdited;  // "edited"
    private Long wordCount; // word_count
    private String status;
    private Boolean isFirstProject;  // "first_project"
    private long dueDate;
    private LocalDateTime deliveryAt;
    private String invitationType;
    private Boolean managedByIPM;
    private String docId;
    private Long corporateId;
    private Long translatorId;
    private String device;
    private String platform;
    private String browser;
    private Long phraseId;
    private String page;

    public GenericPayload() {
    }

    public GenericPayload(JsonNode payload) {
        if (payload == null) {
            return;
        }

        if (payload.has("projectId")) {
            projectId = payload.get("projectId").longValue();
        }
        if (payload.has("project_id")) {
            projectId = payload.get("project_id").longValue();
        }
        if (payload.has("projectName")) {
            projectId = payload.get("projectName").longValue();
        }
        if (payload.has("quoteId")) {
            quoteId = payload.get("quoteId").longValue();
        }
        if (payload.has("quote_id")) {
            quoteId = payload.get("quote_id").longValue();
        }

        if (payload.has("vendorId")) {
            vendorId = payload.get("vendorId").longValue();
        }
        if (payload.has("vendor_id")) {
            vendorId = payload.get("vendor_id").longValue();
        }

        if (payload.has("fileId")) {
            fileId = payload.get("fileId").longValue();
        }
        if (payload.has("file_id")) {
            fileId = payload.get("file_id").longValue();
        }

        if (payload.has("message")) {
            message = payload.get("message").textValue();
        }

        if (payload.has("userId")) {
            setUserId(payload.get("userId").longValue());
        }
        if (payload.has("user_id")) {
            setUserId(payload.get("user_id").longValue());
        }

        if (payload.has("clientId")) {
            clientId = payload.get("clientId").longValue();
        }
        if (payload.has("client_id")) {
            clientId = payload.get("client_id").longValue();
        }

        if (payload.has("targetLanguage")) {
            targetLanguage = payload.get("targetLanguage").textValue();
        }
        if (payload.has("target_language")) {
            targetLanguage = payload.get("target_language").textValue();
        }

        if (payload.has("sourceLanguage")) {
            sourceLanguage = payload.get("sourceLanguage").textValue();
        }
        if (payload.has("source_language")) {
            sourceLanguage = payload.get("source_language").textValue();
        }

        if (payload.has("language") && targetLanguage == null) {
            targetLanguage = payload.get("language").textValue();
        }

        if (payload.has("language") && sourceLanguage == null) {
            sourceLanguage = payload.get("language").textValue();
        }

        if (payload.has("status")) {
            status = payload.get("status").textValue();
        }

        if (payload.has("isFirstProject")) {
            isFirstProject = payload.get("isFirstProject").booleanValue();
        }

        if (payload.has("score")) {
            score = payload.get("score").doubleValue();
        }

        if (payload.has("email")) {
            email = payload.get("email").textValue();
        }

        if (payload.has("edit_reason")) {
            editReason = payload.get("edit_reason").textValue();
        }

        if (payload.has("editReason")) {
            editReason = payload.get("editReason").textValue();
            hasEdited = true;
        } else {
            hasEdited = false;
        }

        if (payload.has("word_count")) {
            wordCount = payload.get("word_count").longValue();
        }

        if (payload.has("wordCount")) {
            wordCount = payload.get("wordCount").longValue();
        }

        if (payload.has("documentId")) {
            documentId = payload.get("documentId").longValue();
        }
        if (payload.has("document_id")) {
            documentId = payload.get("document_id").longValue();
        }
        if (payload.has("docId")) {
            docId = payload.get("docId").textValue();
        }

        if (payload.has("due_date")) {
            dueDate = payload.get("due_date").longValue();
            deliveryAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(payload.get("due_date").longValue()), ZoneId.systemDefault());
        }
        if (payload.has("dueDate")) {
            dueDate = payload.get("dueDate").longValue();
            deliveryAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(payload.get("dueDate").longValue()), ZoneId.systemDefault());
        }

        if (payload.has("invitation_type")) {
            invitationType = payload.get("invitation_type").textValue();
        }

        if (payload.has("invitationType")) {
            invitationType = payload.get("invitationType").textValue();
        }

        if (payload.has("managed_by_ipm")) {
            managedByIPM = payload.get("managed_by_ipm").booleanValue();
        }

        if (payload.has("managedByIPM")) {
            managedByIPM = payload.get("managedByIPM").booleanValue();
        }

        if (payload.has("corporateId")) {
            corporateId = payload.get("corporateId").longValue();
        }

        if (payload.has("corporate_id")) {
            corporateId = payload.get("corporate_id").longValue();
        }

        if (payload.has("translatorId")) {
            translatorId = payload.get("translatorId").longValue();
        }

        if (payload.has("translator_id")) {
            translatorId = payload.get("translator_id").longValue();
        }

        if (payload.has("device")) {
            device = payload.get("device").textValue();
        }

        if (payload.has("platform")) {
            platform = payload.get("platform").textValue();
        }

        if (payload.has("browser")) {
            browser = payload.get("browser").textValue();
        }

        if (payload.has("phraseId")) {
            phraseId = payload.get("phraseId").longValue();
        }

        if (payload.has("phrase_id")) {
            phraseId = payload.get("phrase_id").longValue();
        }

        if (payload.has("page")) {
            page = payload.get("page").textValue();
        }

        if (clientId == null) {
            clientId = userId;
        }

        if (userId == null) {
            userId = clientId;
        }

        if (vendorId != null) {
            if(userId == null) {
                userId = vendorId;
            }

            if(clientId == null) {
                clientId = vendorId;
            }
        }
    }

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public Long getQuoteId() {
        return quoteId;
    }

    public void setQuoteId(Long quoteId) {
        this.quoteId = quoteId;
    }

    public Long getVendorId() {
        return vendorId;
    }

    public void setVendorId(Long vendorId) {
        this.vendorId = vendorId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
        this.user_id = userId;
    }

    public Long getUser_id() {
        return user_id != null ? user_id : userId;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getClientId() {
        return clientId;
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }

    public String getTargetLanguage() {
        return targetLanguage;
    }

    public void setTargetLanguage(String targetLanguage) {
        this.targetLanguage = targetLanguage;
    }

    public String getSourceLanguage() {
        return sourceLanguage;
    }

    public void setSourceLanguage(String sourceLanguage) {
        this.sourceLanguage = sourceLanguage;
    }

    public LanguagePair getLanguagePair() {
        if (this.sourceLanguage == null || this.targetLanguage == null) {
            return null;
        }

        return new LanguagePair(sourceLanguage, targetLanguage);
    }

    public void setLanguagePair(LanguagePair languagePair) {
        this.sourceLanguage = languagePair.getSourceLanguage();
        this.targetLanguage = languagePair.getTargetLanguage();
    }

    public List getSubEvents() {
        return subEvents;
    }

    public void setSubEvents(List subEvents) {
        this.subEvents = subEvents;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Long getDocumentId() {
        return documentId;
    }

    public void setDocumentId(Long documentId) {
        this.documentId = documentId;
    }

    public String getEditReason() {
        return editReason;
    }

    public void setEditReason(String editReason) {
        this.editReason = editReason;
    }

    public Boolean getHasEdited() {
        return hasEdited;
    }

    public void setHasEdited(Boolean hasEdited) {
        this.hasEdited = hasEdited;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getWordCount() {
        return wordCount;
    }

    public void setWordCount(Long wordCount) {
        this.wordCount = wordCount;
    }

    public Boolean getIsFirstProject() {
        return isFirstProject;
    }

    public void setIsFirstProject(Boolean firstProject) {
        isFirstProject = firstProject;
    }

    public LocalDateTime getDeliveryAt() {
        return deliveryAt;
    }

    public void setDeliveryAt(LocalDateTime deliveryAt) {
        this.deliveryAt = deliveryAt;
    }

    public Long getFileId() {
        return fileId;
    }

    public void setFileId(Long fileId) {
        this.fileId = fileId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public long getDueDate() {
        return dueDate;
    }

    public void setDueDate(long dueDate) {
        this.dueDate = dueDate;
    }

    public String getInvitationType() {
        return invitationType;
    }

    public void setInvitationType(String invitationType) {
        this.invitationType = invitationType;
    }

    public Boolean getManagedByIPM() {
        return managedByIPM;
    }

    public void setManagedByIPM(Boolean managedByIPM) {
        this.managedByIPM = managedByIPM;
    }

    public Long getCorporateId() {
        return corporateId;
    }

    public void setCorporateId(Long corporateId) {
        this.corporateId = corporateId;
    }

    public Long getTranslatorId() {
        return translatorId;
    }

    public void setTranslatorId(Long translatorId) {
        this.translatorId = translatorId;
    }


    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public Long getPhraseId() {
        return phraseId;
    }

    public void setPhraseId(Long phraseId) {
        this.phraseId = phraseId;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericPayload payload = (GenericPayload) o;
        return dueDate == payload.dueDate &&
                Objects.equals(projectId, payload.projectId) &&
                Objects.equals(quoteId, payload.quoteId) &&
                Objects.equals(vendorId, payload.vendorId) &&
                Objects.equals(fileId, payload.fileId) &&
                Objects.equals(message, payload.message) &&
                Objects.equals(documentId, payload.documentId) &&
                Objects.equals(userId, payload.userId) &&
                Objects.equals(user_id, payload.user_id) &&
                Objects.equals(clientId, payload.clientId) &&
                Objects.equals(targetLanguage, payload.targetLanguage) &&
                Objects.equals(sourceLanguage, payload.sourceLanguage) &&
                Objects.equals(subEvents, payload.subEvents) &&
                Objects.equals(score, payload.score) &&
                Objects.equals(email, payload.email) &&
                Objects.equals(editReason, payload.editReason) &&
                Objects.equals(hasEdited, payload.hasEdited) &&
                Objects.equals(wordCount, payload.wordCount) &&
                Objects.equals(status, payload.status) &&
                Objects.equals(isFirstProject, payload.isFirstProject) &&
                Objects.equals(deliveryAt, payload.deliveryAt) &&
                Objects.equals(invitationType, payload.invitationType) &&
                Objects.equals(managedByIPM, payload.managedByIPM) &&
                Objects.equals(docId, payload.docId) &&
                Objects.equals(corporateId, payload.corporateId) &&
                Objects.equals(translatorId, payload.translatorId) &&
                Objects.equals(device, payload.device) &&
                Objects.equals(platform, payload.platform) &&
                Objects.equals(browser, payload.browser) &&
                Objects.equals(phraseId, payload.phraseId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, quoteId, vendorId, fileId, message, documentId, userId, user_id, clientId, targetLanguage, sourceLanguage, subEvents, score, email, editReason, hasEdited, wordCount, status, isFirstProject, dueDate, deliveryAt, invitationType, managedByIPM, docId, corporateId, translatorId, device, platform, browser, phraseId);
    }

    @Override
    public String toString() {
        return "GenericPayload{" +
                "projectId=" + projectId +
                ", quoteId=" + quoteId +
                ", vendorId=" + vendorId +
                ", fileId=" + fileId +
                ", message='" + message + '\'' +
                ", documentId=" + documentId +
                ", userId=" + userId +
                ", user_id=" + user_id +
                ", clientId=" + clientId +
                ", targetLanguage='" + targetLanguage + '\'' +
                ", sourceLanguage='" + sourceLanguage + '\'' +
                ", subEvents=" + subEvents +
                ", score=" + score +
                ", email='" + email + '\'' +
                ", editReason='" + editReason + '\'' +
                ", hasEdited=" + hasEdited +
                ", wordCount=" + wordCount +
                ", status='" + status + '\'' +
                ", isFirstProject=" + isFirstProject +
                ", dueDate=" + dueDate +
                ", deliveryAt=" + deliveryAt +
                ", invitationType='" + invitationType + '\'' +
                ", managedByIPM=" + managedByIPM +
                ", docId='" + docId + '\'' +
                ", corporateId=" + corporateId +
                ", translatorId=" + translatorId +
                ", device='" + device + '\'' +
                ", platform='" + platform + '\'' +
                ", browser='" + browser + '\'' +
                ", phraseId=" + phraseId +
                '}';
    }
}
