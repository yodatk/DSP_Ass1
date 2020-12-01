package worker;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;


// Tessaract
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import net.sourceforge.tess4j.util.LoadLibs;

import javax.imageio.ImageIO;

public class OCRParser {


    public String newImageTaskWithTessaract(String urlString, String localId) {
        System.out.println(urlString);

        try {
            Image image = null;
            try {
                URL url = new URL(urlString);
                image = ImageIO.read(url);
            } catch (IOException e) {
                return e.getMessage();
            }
            // the path of your tess data folder
            // inside the extracted file


            Tesseract tesseract = new Tesseract();
            tesseract.setLanguage("eng");
            tesseract.setOcrEngineMode(1);
            File tessDataFolder = LoadLibs.extractTessResources("tessdata");

            tesseract.setDatapath(tessDataFolder.getAbsolutePath());
            return tesseract.doOCR(toBufferedImage(image));

        } catch (TesseractException e) {// | URISyntaxException e) {
            return localId + "_inputfile.txt: Error with parsing: " + e.getMessage();
        } catch (NullPointerException e) {
            return localId + "_inputfile.txt: Image Not found: " + e.getMessage();
        }
    }

    /**
     * Converts a given Image into a BufferedImage
     *
     * @param img The Image to be converted
     * @return The converted BufferedImage
     */
    public BufferedImage toBufferedImage(Image img) {
        if (img instanceof BufferedImage) {
            return (BufferedImage) img;
        }

        // Create a buffered image with transparency
        BufferedImage bimage = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_ARGB);

        // Draw the image on to the buffered image
        Graphics2D bGr = bimage.createGraphics();
        bGr.drawImage(img, 0, 0, null);
        bGr.dispose();

        // Return the buffered image
        return bimage;
    }
}
