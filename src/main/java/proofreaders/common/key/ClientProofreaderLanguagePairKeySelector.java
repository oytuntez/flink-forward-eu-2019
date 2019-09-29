package proofreaders.common.key;

import org.apache.flink.api.java.functions.KeySelector;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;

public class ClientProofreaderLanguagePairKeySelector implements KeySelector<ClientProofreader, ClientLanguagePair> {
    @Override
    public ClientLanguagePair getKey(ClientProofreader value) {
        return new ClientLanguagePair(value.clientId, value.languagePair);
    }
}
