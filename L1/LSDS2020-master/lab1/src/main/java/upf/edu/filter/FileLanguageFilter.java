package upf.edu.filter;

import java.io.IOException;
import java.io.*;

import upf.edu.parser.SimplifiedTweet;
import upf.edu.filter.LanguageFilter;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.Optional;

public class FileLanguageFilter implements LanguageFilter {

    private String inFileName;
    private String outFileName;
    private int countTweets;

    public FileLanguageFilter(String inFileName, String outFileName, int countTweets) {
        this.inFileName = inFileName;
        this.outFileName = outFileName;
        this.countTweets = countTweets;
    }

    public String getInFileName() {
        return inFileName;
    }

    public String getOutFileName() {
        return outFileName;
    }

    public int getCountTweets() {
        return countTweets;
    }

    public void setCountTweets(int countTweets) {
        this.countTweets = countTweets;
    }

    @Override
    public void filterLanguage(String language) {

        BufferedWriter bw = null;
        FileWriter fw = null;

        try {
            // flag true, indica adjuntar información al archivo.
            fw = new FileWriter(getOutFileName(), true);
            bw = new BufferedWriter(fw);

            try {
                //Leemos fichero de entrada
                FileReader file = new FileReader(getInFileName());
                BufferedReader bf = new BufferedReader(file);
                int cont = 0;
                String line;
                while ((line = bf.readLine()) != null) {
                    Optional<SimplifiedTweet> tweet = SimplifiedTweet.fromJson(line);
                    //Filtramos por tweet existente e idioma introducido por parametro                
                    if (tweet.isPresent()) {
                        if (tweet.get().getLanguage().equals(language)) {
                            fw.write(line);//Escribimos en el fichero de salida
                            setCountTweets(getCountTweets() + 1);
                        }
                    }
                    bf.readLine();
                }
                bf.close();
            } catch (IOException e) {
                System.out.println("Error: No se ha podido abrir el fichero de entrada");
                e.printStackTrace();
            }
        } catch (IOException e) {
            System.out.println("Error: No se ha podido crear el fichero de salida");
            e.printStackTrace();
        } finally {
            try {
                //Cierra instancias de FileWriter y BufferedWriter
                if (bw != null) {
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}
