package com.alibaba.alink.server.controller;

import com.alibaba.alink.server.controller.EdgeController.AddEdgeRequest;
import com.alibaba.alink.server.controller.EdgeController.AddEdgeResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest()
@AutoConfigureMockMvc
class EdgeControllerTest {

	protected Gson gson = new GsonBuilder()
		.disableHtmlEscaping()
		.setPrettyPrinting()
		.setDateFormat("yyyy-MM-dd HH:mm:ss")
		.create();

	@Autowired
	protected MockMvc mvc;

	@Test
	public void addEdge() throws Exception {
		AddEdgeRequest req = new EdgeController.AddEdgeRequest();
		req.srcNodeId = 1L;
		req.srcNodePort = 0;
		req.dstNodeId = 2L;
		req.dstNodePort = 0;
		mvc.perform(post("/edge/add")
				.contentType(MediaType.APPLICATION_JSON_VALUE)
				.content(gson.toJson(req)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"))
			.andExpect(jsonPath("$.data.id").isNumber());
	}

	@Test
	public void deleteEdge() throws Exception {
		AddEdgeRequest req = new EdgeController.AddEdgeRequest();
		req.srcNodeId = 1L;
		req.srcNodePort = 0;
		req.dstNodeId = 2L;
		req.dstNodePort = 0;
		MvcResult mvcResult = mvc.perform(post("/edge/add")
				.contentType(MediaType.APPLICATION_JSON_VALUE)
				.content(gson.toJson(req)))
			.andReturn();
		AddEdgeResponse response = gson.fromJson(mvcResult.getResponse().getContentAsString(), AddEdgeResponse.class);
		Long id = response.data.id;
		mvc.perform(get("/edge/del")
				.queryParam("edge_id", id.toString()))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"));
	}

	@Test
	public void deleteEdgeWithIllegalNodeId() throws Exception {
		mvc.perform(get("/edge/del")
				.queryParam("edge_id", "100"))
			.andExpect(jsonPath("$.status").value(BasicResponse.INVALID_ARGUMENT));
	}
}
