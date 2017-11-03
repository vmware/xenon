import com.sun.btrace.*;
import com.sun.btrace.BTraceUtils.Threads;
import com.sun.btrace.annotations.*;

@BTrace
public class CountUriConstructions {
    @OnMethod(
            clazz = "java.net.URI",
            method = "<init>"
    )
    public static void onCreateURI(
            @ProbeClassName String className,
            @ProbeMethodName String probeMethod,
            AnyType[] args) {
        // dump stack trace and later filter only the relevant places using
        // cat CountUriConstructions.class-.default.* | grep parseRequestUri | wc -l
        BTraceUtils.println(Threads.jstackStr(3));
    }
}