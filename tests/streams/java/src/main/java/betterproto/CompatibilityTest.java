package betterproto;

import java.io.IOException;

public class CompatibilityTest {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) throw new RuntimeException("Attempted to run with no argument.");

        Tests tests = new Tests();

        switch (args[0]) {
            case "single_varint":
                tests.testSingleVarint();
                break;

            case "multiple_varints":
                tests.testMultipleVarints();
                break;

            case "single_message":
                tests.testSingleMessage();
                break;

            case "multiple_messages":
                tests.testMultipleMessages();
                break;

            default:
                throw new RuntimeException("Attempted to run with unknown argument '" + args[0] + "'.");
        }
    }
}