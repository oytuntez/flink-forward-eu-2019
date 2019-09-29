package proofreaders.common;

import java.io.Serializable;
import java.util.Objects;

public class ClientProofreader implements Serializable {
    public Long clientId;
    public LanguagePair languagePair;
    public Long vendorId; // userId

    public ClientProofreader() {
    }

    public ClientProofreader(Long clientId, LanguagePair languagePair, Long vendorId) {
        this.clientId = clientId;
        this.languagePair = languagePair;
        this.vendorId = vendorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClientProofreader)) return false;
        ClientProofreader that = (ClientProofreader) o;
        return Objects.equals(clientId, that.clientId) &&
                Objects.equals(languagePair, that.languagePair) &&
                Objects.equals(vendorId, that.vendorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, languagePair, vendorId);
    }

    @Override
    public String toString() {
        return "ClientProofreader{" +
                "clientId=" + clientId +
                ", languagePair=" + languagePair +
                ", vendorId=" + vendorId +
                '}';
    }
}
