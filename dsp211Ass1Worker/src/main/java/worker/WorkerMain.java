package worker;

public class WorkerMain {
    public static void main(String[] args) {

        String[] arr = new String[]{
                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.pnghttp://ct.mob0.com/Fonts/CharacterMap/ocraextended.png",
                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
                "http://www.barcodesoft.com/barcode-image/ocramapping.jpg",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
                "http://files.microscan.com/Technology/OCR/ocr_font_examples.jpg",
                "http://www.idautomation.com/ocr-a-and-ocr-b-fonts/new_sizes_ocr.png",
                "http://www.barcodesoft.com/barcode-image/ocrbrep.png",
                "http://www.selectric.org/selectric/fonts/ansi-ocr.gif",
                "http://luc.devroye.org/OCR-A-Comparison-2009.jpg",
                "http://www.identifont.com/samples/bitstream/OCRA.gif",
                "http://ct.mob0.com/Fonts/CharacterMap/ocraextended-Character-Map.png",
                "http://www.barcodesoft.com/barcode-image/ocramapping.jpg"
        };

        Worker w = new Worker();
        for (String url : arr) {
            System.out.println();
            w.newImageTaskWithTessaract(url);
            System.out.println();
        }

    }
}
