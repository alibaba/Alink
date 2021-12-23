package com.alibaba.alink.server.controller;

import com.alibaba.alink.server.controller.NodeController.AddNodeRequest;
import com.alibaba.alink.server.controller.NodeController.AddNodeResponse;
import com.alibaba.alink.server.controller.NodeParamController.UpdateParamRequest;
import com.alibaba.alink.server.domain.NodeType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest()
@AutoConfigureMockMvc
class NodeParamControllerTest {
	protected Gson gson = new GsonBuilder()
		.disableHtmlEscaping()
		.setPrettyPrinting()
		.setDateFormat("yyyy-MM-dd HH:mm:ss")
		.create();

	@Autowired
	protected MockMvc mvc;

	Long addNode() throws Exception {
		AddNodeRequest req = new NodeController.AddNodeRequest();
		req.nodeName = "shuffle";
		req.positionX = 100.;
		req.positionY = 200.;
		req.className = "com.alibaba.alink.ShuffleBatchOp";
		req.nodeType = NodeType.FUNCTION;
		MvcResult mvcResult = mvc.perform(post("/node/add")
				.contentType(MediaType.APPLICATION_JSON_VALUE)
				.content(gson.toJson(req)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"))
			.andExpect(jsonPath("$.data.id").isNumber())
			.andReturn();
		AddNodeResponse response = gson.fromJson(mvcResult.getResponse().getContentAsString(), AddNodeResponse.class);
		return response.data.id;
	}

	@Test
	public void testGetEmtpyParams() throws Exception {
		Long nodeId = addNode();
		MvcResult mvcResult = mvc.perform(get("/param/get")
				.param("node_id", String.valueOf(nodeId)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"))
			.andExpect(jsonPath("$.data.parameters").value(Matchers.empty()))
			.andReturn();
	}

	@Test
	public void testAddParams() throws Exception {
		Long nodeId = addNode();
		UpdateParamRequest request = new UpdateParamRequest();

		Map <String, String> toUpdate = new HashMap <>();
		toUpdate.put("a", "123");
		toUpdate.put("b", "456");

		request.nodeId = nodeId;
		request.paramsToUpdate = toUpdate;
		request.paramsToDel = new HashSet <>();

		mvc.perform(post("/param/update")
				.content(gson.toJson(request))
				.contentType(MediaType.APPLICATION_JSON_VALUE))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"));

		MvcResult mvcResult = mvc.perform(get("/param/get")
				.param("node_id", String.valueOf(nodeId)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("OK"))
			.andExpect(jsonPath("$.data.parameters").value(Matchers.hasSize(2)))
			.andReturn();
		System.out.println(mvcResult.getResponse().getContentAsString());
	}

	@Test
	public void testDeleteParams() throws Exception {
		Long nodeId = addNode();
		UpdateParamRequest request = new UpdateParamRequest();

		{
			Map <String, String> toUpdate = new HashMap <>();
			toUpdate.put("a", "123");
			toUpdate.put("b", "456");

			request.nodeId = nodeId;
			request.paramsToUpdate = toUpdate;
			request.paramsToDel = new HashSet <>();

			mvc.perform(post("/param/update")
					.content(gson.toJson(request))
					.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("OK"));

			MvcResult mvcResult = mvc.perform(get("/param/get")
					.param("node_id", String.valueOf(nodeId)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("OK"))
				.andExpect(jsonPath("$.data.parameters").value(Matchers.hasSize(2)))
				.andReturn();
		}

		{
			Map <String, String> toUpdate = new HashMap <>();
			toUpdate.put("c", "789");
			Set <String> toDelete = new HashSet <>();
			toDelete.add("a");
			toDelete.add("b");

			request.nodeId = nodeId;
			request.paramsToUpdate = toUpdate;
			request.paramsToDel = toDelete;

			mvc.perform(post("/param/update")
					.content(gson.toJson(request))
					.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("OK"));

			MvcResult mvcResult = mvc.perform(get("/param/get")
					.param("node_id", String.valueOf(nodeId)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("OK"))
				.andExpect(jsonPath("$.data.parameters").value(Matchers.hasSize(1)))
				.andReturn();
		}
	}
}
