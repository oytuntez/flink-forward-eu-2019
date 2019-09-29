package proofreaders.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class ClientLanguagePair implements Serializable {
    private Long clientId;
    private LanguagePair languagePair;

    public ClientLanguagePair() {
    }

    public ClientLanguagePair(Long clientId, LanguagePair languagePair) {
        this.clientId = clientId;
        this.languagePair = languagePair;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClientLanguagePair)) return false;
        ClientLanguagePair that = (ClientLanguagePair) o;
        return clientId.equals(that.clientId) &&
                languagePair.equals(that.languagePair);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, languagePair);
    }

    @Override
    public String toString() {
        return "ClientLanguagePair{" +
                "clientId=" + clientId +
                ", languagePair=" + languagePair +
                '}';
    }
}
