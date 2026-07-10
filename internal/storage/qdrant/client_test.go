package qdrant

import (
	"testing"

	qdrantapi "github.com/qdrant/go-client/qdrant"
)

func TestValidateCollectionSchema(t *testing.T) {
	tests := []struct {
		name string
		info *qdrantapi.CollectionInfo
		want bool
	}{
		{
			name: "compatible",
			info: collectionInfo(qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
				Size: VectorSize, Distance: qdrantapi.Distance_Cosine,
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
				Size: VectorSize, Distance: qdrantapi.Distance_Euclid,
			})),
		},
		{
			name: "named vectors",
			info: collectionInfo(qdrantapi.NewVectorsConfigMap(map[string]*qdrantapi.VectorParams{
				"default": {Size: VectorSize, Distance: qdrantapi.Distance_Cosine},
			})),
		},
		{
			name: "missing config",
			info: &qdrantapi.CollectionInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCollectionSchema("papers", tt.info)
			if (err == nil) != tt.want {
				t.Fatalf("validateCollectionSchema error = %v, want success = %v", err, tt.want)
			}
		})
	}
}

func collectionInfo(vectors *qdrantapi.VectorsConfig) *qdrantapi.CollectionInfo {
	return &qdrantapi.CollectionInfo{
		Config: &qdrantapi.CollectionConfig{
			Params: &qdrantapi.CollectionParams{VectorsConfig: vectors},
		},
	}
}
