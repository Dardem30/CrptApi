import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class CrptApi {
    private final static Logger logger = Logger.getLogger(CrptApi.class.getName());
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static HttpClient client = HttpClient.newHttpClient();
    private final int requestLimit;
    private final long msRequestLimitsThreshold;
    private List<Long> apiCallsLog = new ArrayList<>();

    public CrptApi(TimeUnit interval, int requestLimit) {
        this.requestLimit = requestLimit;
        this.msRequestLimitsThreshold = interval.toMillis(1);
    }

    public static void main(String[] args) {
        final CrptApi crptApi = new CrptApi(TimeUnit.MINUTES, 30);
        try {
            String responseBody = crptApi.executeCall(new Document(), CrptApi.createDocument);
            logger.log(Level.INFO, "Executed call with result [{0}]", responseBody);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to create document", e);
        }
    }

    public static final ThrowableFunction<Document, String> createDocument = document -> {
        HttpRequest request = null;
        String json = null;
        try {
            json = mapper.writeValueAsString(document);
            request = HttpRequest
                    .newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                    .build();
        } catch (JsonProcessingException e) {
            logger.log(Level.WARNING, "Failed to convert POJO to JSON", e);
            throw e;
        }
        final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        logger.log(Level.INFO, "Created Document with status code [" + response.statusCode() + "] for json [" + json + "]", new Object[]{response.statusCode(), json});
        return response.body();
    };

    public synchronized <T, R> T executeCall(R body, ThrowableFunction<R, T> call) throws Exception {
        while (isApiLimitReached()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Thread was interrupted", e);
                throw e;
            }
        }
        try {
            return call.apply(body);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to execute the call", e);
            throw e;
        } finally {
            cleanUpLog();
            apiCallsLog.add(System.currentTimeMillis());
        }
    }

    private void cleanUpLog() {
        final long threshold = System.currentTimeMillis() - msRequestLimitsThreshold;
        this.apiCallsLog = apiCallsLog.stream().filter(callTimeMs -> callTimeMs > threshold).collect(Collectors.toList());
    }

    private boolean isApiLimitReached() {
        final long threshold = System.currentTimeMillis() - msRequestLimitsThreshold;
        int calls = 0;
        for (final Long callTimeMs : apiCallsLog) {
            if (callTimeMs > threshold) {
                calls++;
                if (calls >= requestLimit) {
                    return true;
                }
            }
        }
        return false;
    }

    public class Description {
        private String participantInn;

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }
    }

    public class Product {
        private String certificate_document;
        private String certificate_document_date;
        private String certificate_document_number;
        private String owner_inn;
        private String producer_inn;
        private String production_date;
        private String tnved_code;
        private String uit_code;
        private String uitu_code;

        public String getCertificate_document() {
            return certificate_document;
        }

        public void setCertificate_document(String certificate_document) {
            this.certificate_document = certificate_document;
        }

        public String getCertificate_document_date() {
            return certificate_document_date;
        }

        public void setCertificate_document_date(String certificate_document_date) {
            this.certificate_document_date = certificate_document_date;
        }

        public String getCertificate_document_number() {
            return certificate_document_number;
        }

        public void setCertificate_document_number(String certificate_document_number) {
            this.certificate_document_number = certificate_document_number;
        }

        public String getOwner_inn() {
            return owner_inn;
        }

        public void setOwner_inn(String owner_inn) {
            this.owner_inn = owner_inn;
        }

        public String getProducer_inn() {
            return producer_inn;
        }

        public void setProducer_inn(String producer_inn) {
            this.producer_inn = producer_inn;
        }

        public String getProduction_date() {
            return production_date;
        }

        public void setProduction_date(String production_date) {
            this.production_date = production_date;
        }

        public String getTnved_code() {
            return tnved_code;
        }

        public void setTnved_code(String tnved_code) {
            this.tnved_code = tnved_code;
        }

        public String getUit_code() {
            return uit_code;
        }

        public void setUit_code(String uit_code) {
            this.uit_code = uit_code;
        }

        public String getUitu_code() {
            return uitu_code;
        }

        public void setUitu_code(String uitu_code) {
            this.uitu_code = uitu_code;
        }
    }

    static class Document {
        private Description description;
        private String doc_id;
        private String doc_status;
        private String doc_type;
        private boolean importRequest;
        private String owner_inn;
        private String participant_inn;
        private String producer_inn;
        private String production_date;
        private String production_type;
        private ArrayList<Product> products;
        private String reg_date;
        private String reg_number;

        public Description getDescription() {
            return description;
        }

        public void setDescription(Description description) {
            this.description = description;
        }

        public String getDoc_id() {
            return doc_id;
        }

        public void setDoc_id(String doc_id) {
            this.doc_id = doc_id;
        }

        public String getDoc_status() {
            return doc_status;
        }

        public void setDoc_status(String doc_status) {
            this.doc_status = doc_status;
        }

        public String getDoc_type() {
            return doc_type;
        }

        public void setDoc_type(String doc_type) {
            this.doc_type = doc_type;
        }

        public boolean isImportRequest() {
            return importRequest;
        }

        public void setImportRequest(boolean importRequest) {
            this.importRequest = importRequest;
        }

        public String getOwner_inn() {
            return owner_inn;
        }

        public void setOwner_inn(String owner_inn) {
            this.owner_inn = owner_inn;
        }

        public String getParticipant_inn() {
            return participant_inn;
        }

        public void setParticipant_inn(String participant_inn) {
            this.participant_inn = participant_inn;
        }

        public String getProducer_inn() {
            return producer_inn;
        }

        public void setProducer_inn(String producer_inn) {
            this.producer_inn = producer_inn;
        }

        public String getProduction_date() {
            return production_date;
        }

        public void setProduction_date(String production_date) {
            this.production_date = production_date;
        }

        public String getProduction_type() {
            return production_type;
        }

        public void setProduction_type(String production_type) {
            this.production_type = production_type;
        }

        public ArrayList<Product> getProducts() {
            return products;
        }

        public void setProducts(ArrayList<Product> products) {
            this.products = products;
        }

        public String getReg_date() {
            return reg_date;
        }

        public void setReg_date(String reg_date) {
            this.reg_date = reg_date;
        }

        public String getReg_number() {
            return reg_number;
        }

        public void setReg_number(String reg_number) {
            this.reg_number = reg_number;
        }
    }

    @FunctionalInterface
    public interface ThrowableFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
