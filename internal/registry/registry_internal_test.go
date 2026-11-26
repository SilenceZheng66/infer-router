// registry_internal_test.go — parseMetricValue 指标名匹配规则（含命名空间前缀兼容）。
package registry

import "testing"

func TestParseMetricValue(t *testing.T) {
	const text = `# HELP vllm:gpu_cache_usage_perc KV cache usage
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.873
vllm:num_requests_running 4
engine_inner_queue_depth{engine="0"} 2
unrelated_metric 9.9
`
	cases := []struct {
		name   string
		metric string
		want   float64
		ok     bool
	}{
		{"short name matches prefixed line", "gpu_cache_usage_perc", 0.873, true},
		{"prefixed name exact match", "vllm:gpu_cache_usage_perc", 0.873, true},
		{"prefixed name with labels", "gpu_cache_usage_perc{", 0, false},
		{"labeled metric suffix match", "engine_inner_queue_depth", 2, true},
		{"missing metric", "num_requests_waiting", 0, false},
	}
	for _, c := range cases {
		got, ok := parseMetricValue(text, c.metric)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("%s: parseMetricValue(%q) = (%v, %v), want (%v, %v)", c.name, c.metric, got, ok, c.want, c.ok)
		}
	}
}

func TestParseMetricValueLabeledPrefixed(t *testing.T) {
	// 带命名空间前缀 + 标签的行（SGLang/自研引擎常见形态）
	text := "vllm:gpu_cache_usage_perc{model=\"qwen2.5-1.5b\"} 0.91\n"
	v, ok := parseMetricValue(text, "gpu_cache_usage_perc")
	if !ok || v != 0.91 {
		t.Errorf("labeled prefixed line: got (%v, %v), want (0.91, true)", v, ok)
	}
}

func TestParseMetricValueShortNameExact(t *testing.T) {
	// mock 引擎直接暴露短名（无前缀）——回归保护
	text := "gpu_cache_usage_perc 0.42\n"
	v, ok := parseMetricValue(text, "gpu_cache_usage_perc")
	if !ok || v != 0.42 {
		t.Errorf("short exact line: got (%v, %v), want (0.42, true)", v, ok)
	}
}
