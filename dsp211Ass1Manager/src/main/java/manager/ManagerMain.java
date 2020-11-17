package manager;


import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ManagerMain {


    public static void main(String[] args) {


        HtmlParser htmlParser = new HtmlParser();

        Map<String, String> localsQueues = new HashMap<>();

        if (args.length < 2) {
            System.out.println("AWS key and 'n' must be inserted");
            return;
        }


        String awsKey = args[0];
        //String imagesLocationInS3 = args[1];
        int n = Integer.parseInt(args[1]);

        Manager manager = new Manager(n, awsKey);

    }

}
