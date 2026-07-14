package agent

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/worker"
)

const (
	RerankBatchSize          = 10
	ChunkTypeAbstract        = "abstract"
	EmbeddingStatusCompleted = "completed"
)

type Ranker struct {
	postgres   *postgres.Client
	embedder   *embedding.Generator
	llm        llm.Generator
	structured *llm.StructuredOutput
	cleanup    embeddingCleanupReconciler

	generateFn             func(ctx context.Context, text string) ([]float32, error)
	generateBatchFn        func(ctx context.Context, texts []string) ([][]float32, error)
	storeEmbeddingFn       func(ctx context.Context, emb embedding.PaperEmbedding) error
	searchSimilarFn        func(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*embedding.SearchResult, error)
	getPapersByTopicFn     func(ctx context.Context, topicID string) ([]*postgres.Paper, error)
	updateRelevanceScoreFn func(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error
}

type embeddingCleanupReconciler interface {
	ReconcileEmbeddingCleanup(context.Context, int32) (worker.CleanupResult, error)
}

func NewRanker(pg *postgres.Client, emb *embedding.Generator, llmClient llm.Generator, cleanup embeddingCleanupReconciler) *Ranker {
	ranker := &Ranker{
		postgres:   pg,
		embedder:   emb,
		llm:        llmClient,
		structured: llm.NewStructuredOutput(llmClient),
		cleanup:    cleanup,
	}
	ranker.generateFn = emb.Generate
	ranker.generateBatchFn = emb.GenerateBatch
	ranker.storeEmbeddingFn = emb.StoreEmbedding
	ranker.searchSimilarFn = emb.SearchSimilar
	ranker.getPapersByTopicFn = ranker.getPapersByTopic
	ranker.updateRelevanceScoreFn = ranker.updateRelevanceScore
	return ranker
}

type RankedPaper struct {
	ID             string
	Title          string
	Abstract       string
	PDFURL         string
	RelevanceScore float64
}

type paperWithScore struct {
	paper *postgres.Paper
	score float64
}

func (r *Ranker) Rank(ctx context.Context, topicID string, topic string, maxPapers int) ([]RankedPaper, error) {
	logger.From(ctx).Info().
		Str("topic_id", topicID).
		Int("topic_chars", len(topic)).
		Msg("Starting paper ranking")

	logger.From(ctx).Info().Msg("Embedding topic for Qdrant search")
	topicVector, err := r.generateFn(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to embed topic: %w", err)
	}

	papers, err := r.getPapersByTopicFn(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("failed to get papers: %w", err)
	}

	logger.From(ctx).Info().Int("papers", len(papers)).Msg("Papers retrieved for ranking")

	queryablePapers, err := r.ensureEmbeddings(ctx, topicID, papers)
	if err != nil {
		return nil, err
	}
	if len(queryablePapers) == 0 {
		return nil, fmt.Errorf("no papers with embeddings available for ranking")
	}

	queryLimit := rankQueryLimit(len(queryablePapers), maxPapers)
	searchResults, err := r.searchSimilarFn(ctx, topicVector, uint64(queryLimit), topicID)
	if err != nil {
		return nil, fmt.Errorf("failed to search qdrant: %w", err)
	}

	scored := rankSearchResults(searchResults, queryablePapers)
	if len(scored) == 0 {
		return nil, fmt.Errorf("qdrant returned no ranked papers for topic %s", topicID)
	}

	topK := len(scored)

	logger.From(ctx).Info().Int("papers", topK).Msg("Top papers selected from Qdrant for LLM reranking")

	if r.llm != nil && len(scored) > 0 {
		reranked, err := r.rerankWithLLM(ctx, topic, scored)
		if err != nil {
			return nil, fmt.Errorf("LLM reranking failed: %w", err)
		}
		scored = reranked
	}

	if maxPapers > 0 && len(scored) > maxPapers {
		scored = scored[:maxPapers]
	}

	logger.From(ctx).Info().Int("papers", len(scored)).Msg("Storing relevance scores")

	ranked := make([]RankedPaper, 0, len(scored))
	var persistenceFailures []ItemFailure
	for _, s := range scored {
		ranked = append(ranked, RankedPaper{
			ID:             s.paper.ID.String(),
			Title:          s.paper.Title,
			Abstract:       pgTextVal(s.paper.Abstract),
			PDFURL:         pgTextVal(s.paper.PdfUrl),
			RelevanceScore: s.score,
		})

		if err := r.updateRelevanceScoreFn(ctx, topicID, s.paper.ID, s.score); err != nil {
			logger.From(ctx).Warn().Err(err).Str("paper_id", s.paper.ID.String()).Msg("Failed to update relevance score")
			persistenceFailures = append(persistenceFailures, ItemFailure{Kind: "paper", Identifier: s.paper.ID.String(), Err: err})
		}
	}
	if err := newBatchError("ranking persistence", len(scored), persistenceFailures); err != nil {
		return nil, err
	}

	logger.From(ctx).Info().
		Int("ranked_papers", len(ranked)).
		Msg("Paper ranking complete")

	return ranked, nil
}

func (r *Ranker) ensureEmbeddings(ctx context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
	if r.postgres != nil && r.embedder != nil {
		return r.ensureDurableAbstractEmbeddings(ctx, topicID, papers)
	}
	queryablePapers := make(map[string]*postgres.Paper)
	texts := make([]string, 0, len(papers))
	papersToEmbed := make([]*postgres.Paper, 0, len(papers))

	for _, paper := range papers {
		abstract := pgTextVal(paper.Abstract)
		if abstract == "" {
			logger.From(ctx).Debug().Str("paper_id", paper.ID.String()).Msg("Skipping paper with no abstract")
			continue
		}

		queryablePapers[paper.ID.String()] = paper
		papersToEmbed = append(papersToEmbed, paper)
		texts = append(texts, abstract)
	}

	if len(papersToEmbed) == 0 {
		return queryablePapers, nil
	}

	logger.From(ctx).Info().Int("papers", len(papersToEmbed)).Msg("Generating abstract embeddings for Qdrant")
	vectors, err := r.generateBatchFn(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("failed to generate paper embeddings: %w", err)
	}
	if len(vectors) != len(papersToEmbed) {
		return nil, fmt.Errorf("embedding vector count mismatch: got %d want %d", len(vectors), len(papersToEmbed))
	}

	for i, paper := range papersToEmbed {
		err := r.storeEmbeddingFn(ctx, embedding.PaperEmbedding{
			PaperID:    paper.ID.String(),
			TopicID:    topicID,
			ChunkType:  ChunkTypeAbstract,
			ChunkIndex: 0,
			Text:       texts[i],
			Vector:     vectors[i],
		})
		if err != nil {
			logger.From(ctx).Warn().Err(err).Str("paper_id", paper.ID.String()).Msg("Failed to store embedding in Qdrant")
			delete(queryablePapers, paper.ID.String())
			continue
		}
	}

	return queryablePapers, nil
}

func (r *Ranker) ensureDurableAbstractEmbeddings(ctx context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
	topicUUID, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("invalid topic ID: %w", err)
	}
	queryable := make(map[string]*postgres.Paper)
	persisted := make([]*postgres.PaperChunk, 0, len(papers))
	previous := make(map[uuid.UUID]*postgres.PaperChunk, len(papers))
	for _, paper := range papers {
		chunks, err := r.postgres.Queries().GetPaperChunks(ctx, postgres.GetPaperChunksParams{TopicID: topicUUID, PaperID: paper.ID})
		if err != nil {
			return nil, fmt.Errorf("load existing abstract chunk for paper %s: %w", paper.ID, err)
		}
		for _, chunk := range chunks {
			if chunk.ChunkType == ChunkTypeAbstract && chunk.ChunkIndex == 0 {
				previous[paper.ID] = chunk
				break
			}
		}
	}
	if err := r.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for _, paper := range papers {
			abstract := pgTextVal(paper.Abstract)
			if abstract == "" {
				continue
			}
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(abstract)))
			if old := previous[paper.ID]; old != nil && old.ContentHash != hash && old.QdrantPointID.Valid && old.QdrantCollection.Valid {
				if _, err := q.CreateEmbeddingCleanupTask(ctx, postgres.CreateEmbeddingCleanupTaskParams{
					CollectionName: old.QdrantCollection.String, PointID: uuid.UUID(old.QdrantPointID.Bytes),
					TopicID: topicUUID, PaperID: paper.ID, ChunkID: pgtype.UUID{Bytes: old.ID, Valid: true},
					Reason: "abstract content replaced",
				}); err != nil {
					return fmt.Errorf("schedule old abstract cleanup for paper %s: %w", paper.ID, err)
				}
			}
			chunk, err := q.UpsertPaperChunk(ctx, postgres.UpsertPaperChunkParams{
				TopicID: topicUUID, PaperID: paper.ID, ChunkType: ChunkTypeAbstract, ChunkIndex: 0,
				Text: abstract, ContentHash: hash, Source: "paper_metadata", SectionHeading: pgtype.Text{},
			})
			if err != nil {
				return fmt.Errorf("persist abstract chunk for paper %s: %w", paper.ID, err)
			}
			queryable[paper.ID.String()] = paper
			persisted = append(persisted, chunk)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	identity := r.embedder.Identity()
	collection := r.embedder.CollectionName()
	stale := make([]*postgres.PaperChunk, 0, len(persisted))
	texts := make([]string, 0, len(persisted))
	candidates := make(map[string]*postgres.PaperChunk, len(persisted))
	for _, chunk := range persisted {
		if currentAbstractEmbedding(chunk, identity, collection) {
			candidates[uuid.UUID(chunk.QdrantPointID.Bytes).String()] = chunk
			continue
		}
		stale = append(stale, chunk)
		texts = append(texts, chunk.Text)
	}
	if len(candidates) > 0 {
		pointIDs := make([]string, 0, len(candidates))
		for pointID := range candidates {
			pointIDs = append(pointIDs, pointID)
		}
		existing, err := r.embedder.ExistingPoints(ctx, pointIDs)
		if err != nil {
			return nil, fmt.Errorf("verify abstract vectors in Qdrant: %w", err)
		}
		for pointID, chunk := range candidates {
			if _, ok := existing[pointID]; ok {
				continue
			}
			if _, err := r.postgres.Queries().UpdatePaperChunkEmbeddingStatus(ctx, postgres.UpdatePaperChunkEmbeddingStatusParams{
				TopicID: chunk.TopicID, ID: chunk.ID, EmbeddingStatus: "pending",
				ErrorMessage: pgtype.Text{String: "Qdrant point missing; reindex scheduled", Valid: true},
			}); err != nil {
				return nil, fmt.Errorf("reset missing abstract vector %s: %w", pointID, err)
			}
			stale = append(stale, chunk)
			texts = append(texts, chunk.Text)
		}
	}
	if len(stale) == 0 {
		r.reconcileEmbeddingCleanup(ctx)
		return queryable, nil
	}
	vectors, err := r.embedder.GenerateBatch(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("generate abstract embeddings: %w", err)
	}
	embeddings := make([]embedding.PaperEmbedding, len(stale))
	for index, chunk := range stale {
		embeddings[index] = embedding.PaperEmbedding{
			ChunkID: chunk.ID.String(), PaperID: chunk.PaperID.String(), TopicID: chunk.TopicID.String(),
			ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex), Text: chunk.Text,
			ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String, Identity: identity, Vector: vectors[index],
		}
	}
	if err := r.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range stale {
			pointID, parseErr := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if parseErr != nil {
				return parseErr
			}
			if _, err := q.MarkPaperChunkEmbeddingIndexing(ctx, postgres.MarkPaperChunkEmbeddingIndexingParams{
				TopicID: chunk.TopicID, ID: chunk.ID,
				EmbeddingProvider: pgText(identity.Provider), EmbeddingModel: pgText(identity.Model),
				EmbeddingDimensions:         pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
				EmbeddingInstructionVersion: pgText(identity.InstructionVersion), EmbeddingIndexingVersion: pgText(identity.IndexingVersion),
				QdrantCollection: pgText(collection), QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("mark abstract chunk %s indexing: %w", chunk.ID, err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := r.embedder.StoreEmbeddings(ctx, embeddings); err != nil {
		return nil, fmt.Errorf("store abstract embeddings: %w", err)
	}
	if err := r.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range stale {
			pointID, parseErr := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if parseErr != nil {
				return fmt.Errorf("parse deterministic point ID: %w", parseErr)
			}
			if _, err := q.CompletePaperChunkEmbedding(ctx, postgres.CompletePaperChunkEmbeddingParams{
				TopicID: chunk.TopicID, ID: chunk.ID, QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("complete abstract embedding %s: %w", chunk.ID, err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	r.reconcileEmbeddingCleanup(ctx)
	return queryable, nil
}

func (r *Ranker) reconcileEmbeddingCleanup(ctx context.Context) {
	if r.cleanup == nil {
		return
	}
	result, err := r.cleanup.ReconcileEmbeddingCleanup(ctx, 100)
	if err != nil {
		logger.From(ctx).Warn().Err(err).Int("pending", result.Pending).Msg("Embedding cleanup remains retryable after abstract indexing")
		return
	}
	if result.Completed > 0 || result.Pending > 0 {
		logger.From(ctx).Info().Int("completed", result.Completed).Int("pending", result.Pending).Msg("Reconciled embedding cleanup after abstract indexing")
	}
}

func currentAbstractEmbedding(chunk *postgres.PaperChunk, identity embedding.Identity, collection string) bool {
	return chunk.EmbeddingStatus == EmbeddingStatusCompleted && chunk.EmbeddedContentHash.Valid && chunk.EmbeddedContentHash.String == chunk.ContentHash &&
		chunk.EmbeddingProvider.Valid && chunk.EmbeddingProvider.String == identity.Provider &&
		chunk.EmbeddingModel.Valid && chunk.EmbeddingModel.String == identity.Model &&
		chunk.EmbeddingDimensions.Valid && int(chunk.EmbeddingDimensions.Int32) == identity.Dimensions &&
		chunk.EmbeddingInstructionVersion.Valid && chunk.EmbeddingInstructionVersion.String == identity.InstructionVersion &&
		chunk.EmbeddingIndexingVersion.Valid && chunk.EmbeddingIndexingVersion.String == identity.IndexingVersion &&
		chunk.QdrantCollection.Valid && chunk.QdrantCollection.String == collection && chunk.QdrantPointID.Valid
}

func rankSearchResults(results []*embedding.SearchResult, papersByID map[string]*postgres.Paper) []paperWithScore {
	bestByPaper := make(map[string]paperWithScore)

	for _, result := range results {
		paper, ok := papersByID[result.PaperID]
		if !ok {
			continue
		}

		score := float64(result.Score)
		current, exists := bestByPaper[result.PaperID]
		if !exists || score > current.score {
			bestByPaper[result.PaperID] = paperWithScore{
				paper: paper,
				score: score,
			}
		}
	}

	scored := make([]paperWithScore, 0, len(bestByPaper))
	for _, paper := range bestByPaper {
		scored = append(scored, paper)
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	return scored
}

func rankQueryLimit(totalPapers, maxPapers int) int {
	limit := totalPapers
	if maxPapers > 0 && maxPapers < limit {
		limit = maxPapers
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func (r *Ranker) rerankWithLLM(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	logger.From(ctx).Info().Int("papers", len(papers)).Msg("Starting LLM reranking")

	allScored := make([]paperWithScore, 0, len(papers))
	totalBatches := (len(papers) + RerankBatchSize - 1) / RerankBatchSize

	for i := 0; i < len(papers); i += RerankBatchSize {
		end := i + RerankBatchSize
		if end > len(papers) {
			end = len(papers)
		}
		batch := papers[i:end]
		batchNum := (i / RerankBatchSize) + 1

		logger.From(ctx).Info().
			Int("batch", batchNum).
			Int("total_batches", totalBatches).
			Int("papers_in_batch", len(batch)).
			Msg("Processing rerank batch")

		batchScored, err := r.rerankBatch(ctx, topic, batch)
		if err != nil {
			logger.From(ctx).Warn().Err(err).Int("batch_start", i).Msg("Failed to rerank batch")
			allScored = append(allScored, batch...)
			continue
		}

		allScored = append(allScored, batchScored...)
		logger.From(ctx).Info().Int("batch", batchNum).Msg("Batch reranked successfully")
	}

	sort.Slice(allScored, func(i, j int) bool {
		return allScored[i].score > allScored[j].score
	})

	logger.From(ctx).Info().Int("papers", len(allScored)).Msg("LLM reranking complete")
	return allScored, nil
}

func (r *Ranker) rerankBatch(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	var paperList strings.Builder
	for i, p := range papers {
		abstract := truncateText(pgTextVal(p.paper.Abstract), 500)
		paperList.WriteString(fmt.Sprintf("\n[%d] Title: %s\n    Abstract: %s", i+1, p.paper.Title, abstract))
	}

	prompt := fmt.Sprintf(`You are a research assistant. Rank the following papers by relevance to the given research topic.

Research Topic: %s

Papers:%s

For each paper, provide a relevance score from 0.0 to 1.0 based on:
- Direct relevance to the topic
- Quality of methodology (if discernible from abstract)
- Significance of contribution

IMPORTANT: Respond with ONLY valid JSON. No markdown, no explanations outside JSON.
The response must be a JSON object with a "scores" array.

Example:
{"scores":[{"index":1,"score":0.95,"reason":"highly relevant"},{"index":2,"score":0.75,"reason":"somewhat relevant"}]}

Respond with JSON only:`, topic, paperList.String())

	var response rerankResponse
	err := r.structured.GenerateInto(ctx, prompt, rerankResponse{
		Scores: []scoreEntry{{Index: 1, Score: 0.5, Reason: "reason"}},
	}, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to generate rerank scores: %w", err)
	}

	if err := validateRerankScores(response.Scores, len(papers)); err != nil {
		return nil, fmt.Errorf("failed to parse rerank response: %w", err)
	}

	scoreMap := make(map[int]float64)
	for _, s := range response.Scores {
		scoreMap[s.Index-1] = s.Score
	}

	resultPapers := make([]paperWithScore, len(papers))
	for i, p := range papers {
		resultPapers[i] = paperWithScore{
			paper: p.paper,
			score: p.score,
		}
		if llmScore, ok := scoreMap[i]; ok {
			resultPapers[i].score = 0.3*p.score + 0.7*llmScore
		}
	}

	return resultPapers, nil
}

func (r *Ranker) getPapersByTopic(ctx context.Context, topicID string) ([]*postgres.Paper, error) {
	id, err := parseID("topic ID", topicID)
	if err != nil {
		return nil, err
	}
	return r.postgres.Queries().GetPapersByTopic(ctx, id)
}

func (r *Ranker) updateRelevanceScore(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error {
	id, err := parseID("topic ID", topicID)
	if err != nil {
		return err
	}
	err = r.postgres.Queries().UpdatePaperRelevanceScore(ctx, postgres.UpdatePaperRelevanceScoreParams{
		TopicID:        id,
		PaperID:        paperID,
		RelevanceScore: pgFloat64(score),
	})
	return err
}

type scoreEntry struct {
	Index  int     `json:"index"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

type rerankResponse struct {
	Scores []scoreEntry `json:"scores"`
}

func validateRerankScores(scores []scoreEntry, paperCount int) error {
	if len(scores) == 0 {
		return fmt.Errorf("no scores in response")
	}
	seen := make(map[int]struct{}, len(scores))
	for _, score := range scores {
		if score.Index < 1 || (paperCount > 0 && score.Index > paperCount) {
			return fmt.Errorf("score index out of range: %d", score.Index)
		}
		if math.IsNaN(score.Score) || math.IsInf(score.Score, 0) || score.Score < 0 || score.Score > 1 {
			return fmt.Errorf("score for index %d is outside [0,1]: %g", score.Index, score.Score)
		}
		if _, ok := seen[score.Index]; ok {
			return fmt.Errorf("duplicate score index: %d", score.Index)
		}
		seen[score.Index] = struct{}{}
	}
	if paperCount > 0 && len(seen) != paperCount {
		return fmt.Errorf("expected one score for each of %d papers, got %d", paperCount, len(seen))
	}
	return nil
}
