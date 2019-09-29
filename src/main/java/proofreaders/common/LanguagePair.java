package proofreaders.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class LanguagePair implements Serializable {
    private String sourceLanguage;
    private String targetLanguage;

    public LanguagePair() {
    }

    public LanguagePair(String sourceLanguage, String targetLanguage) {
        this.sourceLanguage = sourceLanguage;
        this.targetLanguage = targetLanguage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LanguagePair)) return false;
        LanguagePair that = (LanguagePair) o;
        return Objects.equals(sourceLanguage, that.sourceLanguage) &&
                Objects.equals(targetLanguage, that.targetLanguage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceLanguage, targetLanguage);
    }

    @Override
    public String toString() {
        return "LanguagePair{" +
                "sourceLanguage='" + sourceLanguage + '\'' +
                ", targetLanguage='" + targetLanguage + '\'' +
                '}';
    }
}
