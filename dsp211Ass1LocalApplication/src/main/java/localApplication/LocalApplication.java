package localApplication;

public class LocalApplication {

    public final static int INPUT_FILE_NAME_INDEX = 0;
    public final static int OUTPUT_FILE_NAME_INDEX = 1;
    public final static int N_INDEX = 2;
    public final static int TERMINATE_INDEX = 3;

    private String inputFileName;
    private String outputFileName;
    private int N;
    private boolean isTerminate;

    public LocalApplication(String inputFileName, String outputFileName, int n, boolean isTerminate) {
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        N = n;
        this.isTerminate = isTerminate;
    }

    public LocalApplication(String[] args) {
        this(
                args[INPUT_FILE_NAME_INDEX],
                args[OUTPUT_FILE_NAME_INDEX],
                Integer.parseInt(args[N_INDEX]),
                (args.length > 3 && args[TERMINATE_INDEX].equals("terminate"))
        );
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public int getN() {
        return N;
    }

    public boolean isTerminate() {
        return isTerminate;
    }

    public void run() {
        if (this.isTerminate) {
            System.out.println("terminate");
        }
        System.out.println("ALL GOOD...");
    }

    /**
     * checks if the argumnents given to the local application is valid.
     *
     * @param args array of strings to check
     * @return String which is the description of what is wrong. if nothing is wrong, return Null
     */
    public static String checkArguments(String[] args) {
        if (args.length == 0) {
            return "ERROR: No arguments were given";
        } else if (args[INPUT_FILE_NAME_INDEX].trim().equals("")) {
            return "ERROR: Input file name cannot be empty";
        } else if (args.length < 2) {
            return "ERROR: Output file name is missing";
        } else if (args[OUTPUT_FILE_NAME_INDEX].trim().equals("")) {
            return "ERROR: Output file name cannot be empty";
        } else if (args.length < 3) {
            return "ERROR: 'n' parameter is missing";
        } else if (!tryParseInt(args[N_INDEX])) {
            return "ERROR: 'n' parameter must be an integer number";
        } else {
            return null;
        }
    }

    /**
     * trying to parse a given string to int if possible
     *
     * @param toCheck string to check if it is an integer
     */
    public static boolean tryParseInt(String toCheck) {
        try {
            Integer.parseInt(toCheck);
            // parsing succeed
            return true;
        } catch (NumberFormatException e) {
            // parsing failed
            return false;
        }
    }
}