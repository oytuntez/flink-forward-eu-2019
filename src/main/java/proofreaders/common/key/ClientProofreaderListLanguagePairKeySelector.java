package proofreaders.common.key;

import org.apache.flink.api.java.functions.KeySelector;
import proofreaders.common.ClientLanguagePair;
import proofreaders.common.ClientProofreader;
import proofreaders.common.ClientProofreadersList;

public class ClientProofreaderListLanguagePairKeySelector implements KeySelector<ClientProofreadersList, ClientLanguagePair> {
    @Override
    public ClientLanguagePair getKey(ClientProofreadersList value) {
        if (value == null || value.size() < 1) {
            return null;
        }

        ClientProofreader first = value.get(0);
        return new ClientLanguagePair(first.clientId, first.languagePair);
    }
}
