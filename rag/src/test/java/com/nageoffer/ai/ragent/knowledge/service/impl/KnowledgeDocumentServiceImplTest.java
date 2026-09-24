/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.knowledge.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.audit.support.BizChangeLogContext;
import com.nageoffer.ai.ragent.core.ingest.IngestionKernel;
import com.nageoffer.ai.ragent.core.ingest.sink.ChunkIndexWriter;
import com.nageoffer.ai.ragent.core.parser.registry.ParserRegistry;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.framework.exception.ServiceException;
import com.nageoffer.ai.ragent.framework.mq.producer.MessageQueueProducer;
import com.nageoffer.ai.ragent.ingestion.dao.mapper.IngestionPipelineMapper;
import com.nageoffer.ai.ragent.ingestion.engine.IngestionEngine;
import com.nageoffer.ai.ragent.ingestion.service.IngestionPipelineService;
import com.nageoffer.ai.ragent.knowledge.config.KnowledgeScheduleProperties;
import com.nageoffer.ai.ragent.knowledge.controller.request.KnowledgeDocumentUploadRequest;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeBaseDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeChunkMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentChunkLogMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.knowledge.handler.RemoteFileFetcher;
import com.nageoffer.ai.ragent.knowledge.service.KnowledgeChunkService;
import com.nageoffer.ai.ragent.knowledge.service.KnowledgeDocumentScheduleService;
import com.nageoffer.ai.ragent.knowledge.support.IngestionSpecCodec;
import com.nageoffer.ai.ragent.knowledge.support.VectorTargetResolver;
import com.nageoffer.ai.ragent.rag.core.vector.VectorStoreService;
import com.nageoffer.ai.ragent.rag.dto.StoredFileDTO;
import com.nageoffer.ai.ragent.rag.service.FileStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.support.TransactionOperations;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KnowledgeDocumentServiceImplTest {

    private static final String KB_ID = "kb-1";
    private static final String COLLECTION = "kb-collection";
    private static final String FILE_KEY = "kb-collection/file.pdf";

    @Mock
    private KnowledgeBaseMapper knowledgeBaseMapper;
    @Mock
    private KnowledgeDocumentMapper documentMapper;
    @Mock
    private ParserRegistry parserRegistry;
    @Mock
    private IngestionSpecCodec ingestionSpecCodec;
    @Mock
    private FileStorageService fileStorageService;
    @Mock
    private KnowledgeScheduleProperties scheduleProperties;
    @Mock
    private RemoteFileFetcher remoteFileFetcher;
    @Mock
    private BizChangeLogContext bizChangeLogContext;

    private KnowledgeDocumentServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new KnowledgeDocumentServiceImpl(
                knowledgeBaseMapper, documentMapper, parserRegistry,
                mock(IngestionKernel.class), mock(ChunkIndexWriter.class), ingestionSpecCodec,
                fileStorageService, mock(VectorStoreService.class), mock(KnowledgeChunkService.class),
                new ObjectMapper(), mock(KnowledgeDocumentScheduleService.class),
                mock(IngestionPipelineService.class), mock(IngestionPipelineMapper.class),
                mock(IngestionEngine.class), mock(KnowledgeDocumentChunkLogMapper.class),
                mock(KnowledgeChunkMapper.class), mock(TransactionOperations.class),
                mock(MessageQueueProducer.class), scheduleProperties, remoteFileFetcher,
                mock(VectorTargetResolver.class), bizChangeLogContext);
        when(knowledgeBaseMapper.selectById(KB_ID)).thenReturn(KnowledgeBaseDO.builder()
                .id(KB_ID).collectionName(COLLECTION).build());
        when(ingestionSpecCodec.normalize(any())).thenReturn("{}");
        when(parserRegistry.canParse("application/pdf")).thenReturn(true);
    }

    @Test
    void shouldDeleteUploadedFileWhenInsertReturnsZero() {
        MockMultipartFile file = prepareFileUpload();
        when(documentMapper.insert(any(KnowledgeDocumentDO.class))).thenReturn(0);

        assertThrows(ServiceException.class, () -> service.upload(KB_ID, fileRequest(), file));

        verify(fileStorageService).deleteByUrl(FILE_KEY);
    }

    @Test
    void shouldPreserveUnsupportedFileErrorWhenCleanupFails() {
        MockMultipartFile file = prepareFileUpload();
        when(parserRegistry.canParse("application/pdf")).thenReturn(false);
        doThrow(new IllegalStateException("storage unavailable"))
                .when(fileStorageService).deleteByUrl(FILE_KEY);

        ClientException thrown = assertThrows(ClientException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertEquals("暂不支持的文件类型：pdf", thrown.getMessage());
        verify(fileStorageService).deleteByUrl(FILE_KEY);
        verify(documentMapper, never()).insert(any(KnowledgeDocumentDO.class));
    }

    @Test
    void shouldDeleteUploadedUrlWhenInsertFailureIsConfirmedAbsent() {
        prepareUrlUpload();
        RuntimeException insertFailure = new IllegalStateException("insert failed");
        failInsertWithAssignedId(insertFailure);
        when(documentMapper.selectById("doc-1")).thenReturn(null);

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, urlRequest(), null));

        assertSame(insertFailure, thrown);
        verify(fileStorageService).deleteByUrl(FILE_KEY);
    }

    @Test
    void shouldKeepUploadedFileWhenInsertStateCannotBeVerified() {
        MockMultipartFile file = prepareFileUpload();
        RuntimeException insertFailure = new IllegalStateException("insert failed");
        failInsertWithAssignedId(insertFailure);
        when(documentMapper.selectById("doc-1")).thenThrow(new IllegalStateException("database unavailable"));

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertSame(insertFailure, thrown);
        verify(fileStorageService, never()).deleteByUrl(anyString());
    }

    @Test
    void shouldKeepUploadedFileWhenInsertFailsBeforeDocumentIdIsAvailable() {
        MockMultipartFile file = prepareFileUpload();
        RuntimeException insertFailure = new IllegalStateException("insert failed");
        when(documentMapper.insert(any(KnowledgeDocumentDO.class))).thenThrow(insertFailure);

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertSame(insertFailure, thrown);
        verify(documentMapper, never()).selectById(anyString());
        verify(fileStorageService, never()).deleteByUrl(anyString());
    }

    @Test
    void shouldKeepUploadedFileWhenFailedInsertActuallyPersisted() {
        MockMultipartFile file = prepareFileUpload();
        RuntimeException insertFailure = new IllegalStateException("insert acknowledgement lost");
        failInsertWithAssignedId(insertFailure);
        when(documentMapper.selectById("doc-1")).thenReturn(KnowledgeDocumentDO.builder()
                .id("doc-1").fileUrl(FILE_KEY).build());

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertSame(insertFailure, thrown);
        verify(fileStorageService, never()).deleteByUrl(anyString());
    }

    @Test
    void shouldPreserveInsertFailureWhenCleanupFails() {
        MockMultipartFile file = prepareFileUpload();
        RuntimeException insertFailure = new IllegalStateException("insert failed");
        failInsertWithAssignedId(insertFailure);
        when(documentMapper.selectById("doc-1")).thenReturn(null);
        doThrow(new IllegalStateException("storage unavailable"))
                .when(fileStorageService).deleteByUrl(FILE_KEY);

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertSame(insertFailure, thrown);
        verify(fileStorageService).deleteByUrl(FILE_KEY);
    }

    @Test
    void shouldNotDeleteStoredFileWhenAuditContextFailsAfterInsert() {
        MockMultipartFile file = prepareFileUpload();
        RuntimeException auditFailure = new IllegalStateException("audit failed");
        when(documentMapper.insert(any(KnowledgeDocumentDO.class))).thenAnswer(invocation -> {
            KnowledgeDocumentDO document = invocation.getArgument(0);
            document.setId("doc-1");
            return 1;
        });
        doThrow(auditFailure).when(bizChangeLogContext).putName(anyString());

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.upload(KB_ID, fileRequest(), file));

        assertSame(auditFailure, thrown);
        verify(fileStorageService, never()).deleteByUrl(anyString());
    }

    private MockMultipartFile prepareFileUpload() {
        MockMultipartFile file = new MockMultipartFile(
                "file", "file.pdf", "application/pdf", "pdf".getBytes(StandardCharsets.UTF_8));
        when(fileStorageService.upload(COLLECTION, file)).thenReturn(storedFile());
        return file;
    }

    private void prepareUrlUpload() {
        when(remoteFileFetcher.fetchAndStore(COLLECTION, "https://example.com/file.pdf"))
                .thenReturn(storedFile());
    }

    private void failInsertWithAssignedId(RuntimeException failure) {
        when(documentMapper.insert(any(KnowledgeDocumentDO.class))).thenAnswer(invocation -> {
            KnowledgeDocumentDO document = invocation.getArgument(0);
            document.setId("doc-1");
            throw failure;
        });
    }

    private KnowledgeDocumentUploadRequest fileRequest() {
        KnowledgeDocumentUploadRequest request = new KnowledgeDocumentUploadRequest();
        request.setSourceType("file");
        request.setProcessMode("chunk");
        return request;
    }

    private KnowledgeDocumentUploadRequest urlRequest() {
        KnowledgeDocumentUploadRequest request = new KnowledgeDocumentUploadRequest();
        request.setSourceType("url");
        request.setSourceLocation("https://example.com/file.pdf");
        request.setProcessMode("chunk");
        return request;
    }

    private StoredFileDTO storedFile() {
        return StoredFileDTO.builder()
                .url(FILE_KEY)
                .detectedType("pdf")
                .mimeType("application/pdf")
                .size(3L)
                .originalFilename("file.pdf")
                .build();
    }
}
