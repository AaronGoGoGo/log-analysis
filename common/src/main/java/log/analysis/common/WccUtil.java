package log.analysis.common;

import org.wcc.crypt.Crypter;
import org.wcc.crypt.CrypterFactory;
import org.wcc.framework.AppContext;

public class WccUtil {
    private static WccUtil instance = new WccUtil();

    private WccUtil() {
        AppContext.getInstance().setAppHomePath("/opt/client/wcc/config");
    }

    public static final WccUtil getInstance() {

        return instance;
    }

    public synchronized String decrypt(String src) {
        Crypter crypter = CrypterFactory.getCrypter(CrypterFactory.AES_CBC);
        return crypter.decrypt(src);
    }

    static public String encrypt(String src) {
        Crypter crypter = CrypterFactory.getCrypter(CrypterFactory.AES_CBC);
        return crypter.encrypt(src);
    }
}
