package qdrant

import (
	"testing"

	"github.com/google/uuid"
	qdrantapi "github.com/qdrant/go-client/qdrant"
)

// Protects validate collection schema.
func TestValidateCollectionSchema(t *testing.T) {
	const dimensions = 4096
	tests := []struct {
		name string
		info *qdrantapi.CollectionInfo
		want bool
	}{
		{
			name: "compatible",
			info: collectionInfo(qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
				Size: dimensions, Distance: qdrantapi.Distance_Cosine,
			})),
			want: true,
		},
		{
			name: "wrong size",
			info: collectionInfo(qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
				Size: 384, Distance: qdrantapi.Distance_Cosine,
			})),
		},
		{
			name: "wrong distance",
			info: collectionInfo(qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
				Size: dimensions, Distance: qdrantapi.Distance_Euclid,
			})),
		},
		{
			name: "named vectors",
			info: collectionInfo(qdrantapi.NewVectorsConfigMap(map[string]*qdrantapi.VectorParams{
				"default": {Size: dimensions, Distance: qdrantapi.Distance_Cosine},
			})),
		},
		{
			name: "missing config",
			info: &qdrantapi.CollectionInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCollectionSchema("papers", tt.info, dimensions)
			if (err == nil) != tt.want {
				t.Fatalf("validateCollectionSchema error = %v, want success = %v", err, tt.want)
			}
		})
	}
}

// Protects repeated embedding identities receive distinct physical generations.
func TestGenerationCollectionNameIncludesGenerationID(t *testing.T) {
	first := GenerationCollectionName("paper_embeddings", "qwen3", uuid.New())
	second := GenerationCollectionName("paper_embeddings", "qwen3", uuid.New())
	if first == second {
		t.Fatalf("generation collection names match: %q", first)
	}
	if want := "paper_embeddings_qwen3_"; len(first) <= len(want) || first[:len(want)] != want {
		t.Fatalf("generation collection name = %q, want prefix %q", first, want)
	}
}

func collectionInfo(vectors *qdrantapi.VectorsConfig) *qdrantapi.CollectionInfo {
	return &qdrantapi.CollectionInfo{
		Config: &qdrantapi.CollectionConfig{
			Params: &qdrantapi.CollectionParams{VectorsConfig: vectors},
		},
	}
}

// Protects mutation requests wait for application.
func TestMutationRequestsWaitForApplication(t *testing.T) {
	upsert := upsertRequest("papers", []*qdrantapi.PointStruct{})
	deleted := deleteIDsRequest("papers", []*qdrantapi.PointId{})
	if !upsert.GetWait() || !deleted.GetWait() {
		t.Fatalf("mutation wait flags = %v/%v, want true/true", upsert.GetWait(), deleted.GetWait())
	}
}

// Protects vector queries return the payload fields consumed by ranking and chunk retrieval.
func TestQueryRequestIncludesRequiredPayload(t *testing.T) {
	request := queryRequest("papers", []float32{0.1, 0.2}, 20, nil)
	include := request.GetWithPayload().GetInclude().GetFields()
	want := []string{"paper_id", "chunk_type", "chunk_index", "text"}
	if len(include) != len(want) {
		t.Fatalf("payload include = %#v, want %#v", include, want)
	}
	for index, field := range want {
		if include[index] != field {
			t.Errorf("payload include[%d] = %q, want %q", index, include[index], field)
		}
	}
}

// Protects activation actions switch alias atomically.
func TestActivationActionsSwitchAliasAtomically(t *testing.T) {
	actions := activationActions("current", "generation-old", "generation-new")
	if len(actions) != 2 {
		t.Fatalf("activation actions = %d, want delete-alias and create-alias", len(actions))
	}
	deleted := actions[0].GetDeleteAlias()
	created := actions[1].GetCreateAlias()
	if deleted == nil || deleted.GetAliasName() != "current" {
		t.Fatalf("delete action = %#v, want current alias", deleted)
	}
	if created == nil || created.GetAliasName() != "current" || created.GetCollectionName() != "generation-new" {
		t.Fatalf("create action = %#v, want current -> generation-new", created)
	}
}
