package worker;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;

// Aprise OCR
import com.asprise.ocr.Ocr;


// Tessaract
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import javax.imageio.ImageIO;

public class Worker {

    public void newImageTask(String urlString) {
        Ocr.setUp();
        Ocr ocr = new Ocr();
        ocr.startEngine("eng", Ocr.SPEED_SLOW);
        RenderedImage img = null;
        try {
            URL url = new URL(urlString);
            img = ImageIO.read(url);
        } catch (IOException e) {
            System.out.println("Didn't work");
        }
        if (img != null) {
            String text = ocr.recognize(img, Ocr.RECOGNIZE_TYPE_TEXT, Ocr.OUTPUT_FORMAT_PLAINTEXT);
            System.out.printf("url: %s\n", urlString);
            System.out.printf("recognized text: %s\n", text);
        }

    }

    public void newImageTaskWithTessaract(String urlString) {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        try {
            Image image = null;
            try {
                URL url = new URL(urlString);
                image = ImageIO.read(url);
            } catch (IOException e) {
                System.out.println("Didn't work");
            }
            // the path of your tess data folder
            // inside the extracted file
            if (image != null) {
                Tesseract tesseract = new Tesseract();
                tesseract.setDatapath("/tessdata");
                tesseract.setLanguage("eng");
                tesseract.setPageSegMode(1);
                tesseract.setOcrEngineMode(1);
                String text
                        = tesseract.doOCR(toBufferedImage(image));

                // path of your image file
                System.out.print(text);
            }

        } catch (TesseractException e) {
            e.printStackTrace();
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
