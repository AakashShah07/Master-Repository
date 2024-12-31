package com.example.Master.controller;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/master")
public class masterController {

    // List of worker node URLs
    private List<String> workerNodes = List.of("http://localhost:8080");
    private int currentWorker = 0; // Round-robin counter for load balancing

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            // Assign the file to the next available worker node
            String workerUrl = getNextWorkerNode() + "/file/upload";
            String response = sendFileToWorker(file, workerUrl);

            return ResponseEntity.ok("File processed successfully. Response: " + response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing file: " + e.getMessage());
        }
    }

    @GetMapping("/downloadOriginal/{fileId}")
    public ResponseEntity<byte[]> downloadOriginalFile(@PathVariable String fileId) {
        try {
            // Forward request to the worker node
            String workerUrl = getNextWorkerNode() + "/file/downloadOriginal/" + fileId;
            return fetchFileFromWorker(workerUrl);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @GetMapping("/download/{fileId}")
    public ResponseEntity<byte[]> downloadFile(@PathVariable String fileId) {
        try {
            // Forward request to the worker node
            String workerUrl = getNextWorkerNode() + "/file/download/" + fileId;
            return fetchFileFromWorker(workerUrl);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    // Logic to get the next worker node in a round-robin fashion
    private String getNextWorkerNode() {
        String worker = workerNodes.get(currentWorker);
        currentWorker = (currentWorker + 1) % workerNodes.size(); // Increment and wrap around
        return worker;
    }

    // Send the file to the worker node
    private String sendFileToWorker(MultipartFile file, String workerUrl) throws IOException {
        RestTemplate restTemplate = new RestTemplate();

        // Create the multipart request
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", new ByteArrayResource(file.getBytes()) {
            @Override
            public String getFilename() {
                return file.getOriginalFilename();
            }
        });

        //aakashshah0707
        //fcexqka8le8BHXVF

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        // Send POST request to the worker node
        ResponseEntity<String> response = restTemplate.postForEntity(workerUrl, requestEntity, String.class);
        return response.getBody();
    }

    // Fetch file from the worker node
    private ResponseEntity<byte[]> fetchFileFromWorker(String workerUrl) {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<byte[]> response = restTemplate.getForEntity(workerUrl, byte[].class);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        // Extract filename from worker's Content-Disposition header
        String contentDisposition = response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION);
        if (contentDisposition != null) {
            headers.set(HttpHeaders.CONTENT_DISPOSITION, contentDisposition);
        }

        return new ResponseEntity<>(response.getBody(), headers, HttpStatus.OK);
    }
}
