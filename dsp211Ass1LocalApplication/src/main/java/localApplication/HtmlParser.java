package localApplication;

//region imports

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

//endregion imports

/**
 * Class that is responsible for converting the incoming data from manager (couples of images and their parsed text)
 * to proper HTML file
 */
public class HtmlParser {

    /**
     * Html header to to write in the start of the output html file
     */
    public static final String HTML_HEADER = "<html>\n\t<head>\n\t<meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1252\"><title>OCR</title>\n\t</head>\n\t<body>\n";
    /**
     * Html footer to to write in the end of the output html file
     */
    public static final String HTML_FOOTER = "\t</body>\n</html>";
    /**
     * HTML tags to start paragraph and image in out put file
     */
    public static final String HTML_START_PARAGRAPH_AND_IMAGE = "\t\t<p>\n" + "\t\t<img src=";
    /**
     * HTML tags to end image and start parsed text in ht
     */
    public static final String HTML_AFTER_IMAGE_BEFORE_TEXT = "><br>\n\t\t";
    /**
     * Html tags constant to put after each image parsing
     */
    public static final String HTML_AFTER_TEXT = "\n\t\t</p>\n";


    /**
     * File object which is the requested output html file
     */
    private File htmlFile;
    /**
     * String which is the name of the output file
     */
    private String fileName;

    /**
     * Getter for the html file
     *
     * @return File which is the output File so far
     */
    public File getHtmlFile() {
        return htmlFile;
    }

    /**
     * Getter for the file name
     *
     * @return String represent the name of the output file
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Constructor for the html parser
     *
     * @param fileName String which is the wanted name for the output file
     */
    public HtmlParser(String fileName) {
        this.fileName = fileName + ".html";
        this.htmlFile = new File(this.fileName);

    }

    /**
     * Adding all the necessary init tags for the html file so it would be a valid html file to show on browser
     *
     * @return boolean: true if the tags were written properly, false otherwise
     */
    public boolean initFile() {
        return this.writeConstantToFile(HTML_HEADER);
    }

    /**
     * Helper function to write constant tags to output html file
     *
     * @param toWrite String which is the current tags needed to write to file (HTML_HEADER or HTML_FOOTER)
     * @return boolean: true if the string was written to the file properly, false if there was error of some kind
     */
    private boolean writeConstantToFile(String toWrite) {
        try {
            // writing needed tags
            BufferedWriter bw = new BufferedWriter(new FileWriter(this.htmlFile, true));
            bw.write(toWrite);
            bw.close();
            return true;
        } catch (IOException e) {
            System.out.println("problem with io");
            e.printStackTrace();
            return false;
        }
    }


    /**
     * Adding all the necessary end tags for the html file so it would be a valid html file to show on browser
     *
     * @return boolean: true if the tags were written properly, false otherwise
     */
    public boolean endFile() {
        return this.writeConstantToFile(HTML_FOOTER);
    }

    /**
     * Appending current images with parsed text to the html file
     *
     * @param urlsToText List of Pairs of String urls to String parsed text from image.
     * @return boolean : true if the urls and parsed text with thier tags have been written properly, false otherwise
     */
    public boolean appendListOfUrlAndTextToHTML(List<Pair<String, String>> urlsToText) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(this.htmlFile, true));
            for (Pair<String, String> imageWithParse : urlsToText) {

                String paragraph = HTML_START_PARAGRAPH_AND_IMAGE + imageWithParse.getKey() +
                        HTML_AFTER_IMAGE_BEFORE_TEXT +
                        imageWithParse.getValue() +
                        HTML_AFTER_TEXT;
                bw.write(paragraph);
            }
            bw.close();
        } catch (IOException e) {
            System.out.println("problem with io");
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
