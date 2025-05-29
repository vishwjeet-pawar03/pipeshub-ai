import { Icon } from '@iconify/react';
import { useParams, useNavigate } from 'react-router-dom';
import { useMemo, useState, useEffect, useCallback } from 'react';

import {
  Box,
  Card,
  Chip,
  alpha,
  Button,
  Divider,
  useTheme,
  Typography,
  CardContent,
  LinearProgress,
  CircularProgress,
} from '@mui/material';

import axios from 'src/utils/axios';

import { CONFIG } from 'src/config-global';

import SearchResultItem from './search-result';
import { ORIGIN } from './constants/knowledge-search';
import { SearchQueryMetadata } from './search-query-meta';
import ExcelViewer from '../qna/chatbot/components/excel-highlighter';
import PdfHighlighterComp from '../qna/chatbot/components/pdf-highlighter'; // Import the new component

import { StyledCloseButton } from './knowledge-base-search';

import type { Filters } from './types/knowledge-base';
import type { PipesHub, SearchResult, AggregatedDocument } from './types/search-response';

// Constants for sidebar widths - must match with the sidebar component
const SIDEBAR_EXPANDED_WIDTH = 300;
const SIDEBAR_COLLAPSED_WIDTH = 64;

export default function SearchHistoryDetails() {
  const { id } = useParams<{ id: string }>();
  const theme = useTheme();
  const navigate = useNavigate();

  const [filters, setFilters] = useState<Filters>({
    department: [],
    moduleId: [],
    appSpecificRecordType: [],
  });
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [aggregatedCitations, setAggregatedCitations] = useState<AggregatedDocument[]>([]);
  const [openSidebar, setOpenSidebar] = useState<boolean>(true);
  const [isPdf, setIsPdf] = useState<boolean>(false);
  const [isExcel, setIsExcel] = useState<boolean>(false);
  const [fileUrl, setFileUrl] = useState<string>('');
  const [recordCitations, setRecordCitations] = useState<AggregatedDocument | null>(null);
  const [hasSearched, setHasSearched] = useState<boolean>(false);
  const [recordsMap, setRecordsMap] = useState<Record<string, PipesHub.Record>>({});
  const [fileBuffer, setFileBuffer] = useState<ArrayBuffer | null>(null);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [topK, setTopK] = useState<number>(10);
  const [error, setError] = useState<string | null>(null);
  const [searchHistory, setSearchHistory] = useState<any | null>(null);
  const [activeRecordId, setActiveRecordId] = useState<string | null>(null);

  // Add a state to track if citation viewer is open
  const isCitationViewerOpen = isPdf || isExcel;

  const handleFilterChange = (newFilters: Filters) => {
    setFilters(newFilters);
  };

  const handleRepeatSearch = () => {
    if (searchQuery) {
      navigate(`/`);
    }
  };

  const aggregateCitationsByRecordId = useCallback(
    (documents: SearchResult[]): AggregatedDocument[] => {
      // Create a map to store aggregated documents
      const aggregationMap = documents.reduce(
        (acc, doc) => {
          const recordId = doc.metadata?.recordId || 'unknown';

          // If this recordId doesn't exist in the accumulator, create a new entry
          if (!acc[recordId]) {
            acc[recordId] = {
              recordId,
              documents: [],
            };
          }

          // Add current document to the group
          acc[recordId].documents.push(doc);

          return acc;
        },
        {} as Record<string, AggregatedDocument>
      );

      // Convert the aggregation map to an array
      return Object.values(aggregationMap);
    },
    []
  );

  const aggregateRecordsByRecordId = useCallback(
    (records: PipesHub.Record[]): Record<string, PipesHub.Record> =>
      records.reduce(
        (acc, record) => {
          // Use _key as the lookup key
          const recordKey = record._key || 'unknown';

          // Store the record in the accumulator with its _key as the key
          acc[recordKey] = record;

          return acc;
        },
        {} as Record<string, PipesHub.Record>
      ),
    []
  );

  // Fetch search history by ID
  useEffect(() => {
    const fetchSearchHistoryDetails = async () => {
      setLoading(true);
      setError(null);
      setHasSearched(true);

      try {
        // Fetch the search history data using the ID from URL params
        const response = await axios.get(`/api/v1/search/${id}`);

        // Handle data format
        const searchData = Array.isArray(response.data) ? response.data[0] : response.data;

        if (!searchData) {
          throw new Error('No search data found in response');
        }

        // Store the full search history data
        setSearchHistory(searchData);

        // Set the search query from history
        setSearchQuery(searchData.query || '');

        // Set top K value if available
        if (searchData.limit) {
          setTopK(searchData.limit);
        }

        // Set search results - these will be the citations from the history
        const results = Array.isArray(searchData.citationIds) ? searchData.citationIds : [];
        setSearchResults(results);

        // Extract record information
        const recordsData: Record<string, any> = searchData.records || {};

        // Transform records data format to match what's expected by the component
        const recordsList = Object.entries(recordsData).map(([key, value]) => {
          // Parse record data if it's a string
          const recordData = typeof value === 'string' ? JSON.parse(value) : value;

          // Extract record ID from the key (assumes format like 'files/{recordId}')
          const recordId = key.split('/').pop() || '';

          return {
            _key: recordId,
            ...recordData,
          };
        });

        // Set records map
        const recordsLookupMap = aggregateRecordsByRecordId(recordsList);
        setRecordsMap(recordsLookupMap);

        // Aggregate citations
        const citations = aggregateCitationsByRecordId(results);
        setAggregatedCitations(citations);
      } catch (error) {
        console.error('Failed to fetch search history:', error);
        setError('Failed to load search history details');
        setSearchResults([]);
        setAggregatedCitations([]);
      } finally {
        setLoading(false);
      }
    };

    if (id) {
      fetchSearchHistoryDetails();
    }
  }, [id, aggregateCitationsByRecordId, aggregateRecordsByRecordId]);

  const viewCitations = async (recordId: string, isDocPdf: boolean, isDocExcel: boolean) => {
    // Reset view states
    setIsPdf(false);
    setIsExcel(false);
    setActiveRecordId(recordId);

    if (isDocPdf) {
      setIsPdf(true);
    } else if (isDocExcel) {
      setIsExcel(true);
    } else {
      return; // If neither PDF nor Excel, don't proceed
    }

    // Close sidebar when showing citation viewer
    setOpenSidebar(false);

    try {
      // Get the record ID from parameter or fallback
      const record = recordsMap[recordId];

      // If record doesn't exist, use fallback or show error
      if (!record) {
        console.error('Record not found for ID:', recordId);
        return;
      }
      // Find the correct citation from the aggregated data
      const citation = aggregatedCitations.find((item) => item.recordId === recordId);
      if (citation) {
        setRecordCitations(citation);
      }
      if (record.origin === ORIGIN.UPLOAD) {
        const fetchRecordId = record.externalRecordId || '';
        if (!fetchRecordId) {
          console.error('No external record ID available');
          return;
        }

        try {
          const response = await axios.get(`/api/v1/document/${fetchRecordId}/download`, {
            responseType: 'blob',
          });

          // Read the blob response as text to check if it's JSON with signedUrl
          const reader = new FileReader();
          const textPromise = new Promise<string>((resolve) => {
            reader.onload = () => {
              resolve(reader.result?.toString() || '');
            };
          });

          reader.readAsText(response.data);
          const text = await textPromise;

          let filename = record.recordName || `document-${record.recordId}`;
          const contentDisposition = response.headers['content-disposition'];
          if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]*)"?/);
            if (filenameMatch && filenameMatch[1]) {
              filename = filenameMatch[1];
            }
          }

          try {
            // Try to parse as JSON to check for signedUrl property
            const jsonData = JSON.parse(text);
            if (jsonData && jsonData.signedUrl) {
              setFileUrl(jsonData.signedUrl);
            }
          } catch (e) {
            // Case 2: Local storage - Return buffer
            const bufferReader = new FileReader();
            const arrayBufferPromise = new Promise<ArrayBuffer>((resolve) => {
              bufferReader.onload = () => {
                resolve(bufferReader.result as ArrayBuffer);
              };
              bufferReader.readAsArrayBuffer(response.data);
            });

            const buffer = await arrayBufferPromise;
            setFileBuffer(buffer);
          }
        } catch (error) {
          console.error('Error downloading document:', error);
          throw new Error('Failed to download document');
        }
      } else if (record.origin === ORIGIN.CONNECTOR) {
        try {
          const response = await axios.get(
            `${CONFIG.backendUrl}/api/v1/knowledgeBase/stream/record/${recordId}`,
            {
              responseType: 'blob',
            }
          );

          // Extract filename from content-disposition header
          let filename = record.recordName || `document-${recordId}`;
          const contentDisposition = response.headers['content-disposition'];
          if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]*)"?/);
            if (filenameMatch && filenameMatch[1]) {
              filename = filenameMatch[1];
            }
          }

          // Convert blob directly to ArrayBuffer
          const bufferReader = new FileReader();
          const arrayBufferPromise = new Promise<ArrayBuffer>((resolve, reject) => {
            bufferReader.onload = () => {
              // Create a copy of the buffer to prevent detachment issues
              const originalBuffer = bufferReader.result as ArrayBuffer;
              const bufferCopy = originalBuffer.slice(0);
              resolve(bufferCopy);
            };
            bufferReader.onerror = () => {
              reject(new Error('Failed to read blob as array buffer'));
            };
            bufferReader.readAsArrayBuffer(response.data);
          });

          const buffer = await arrayBufferPromise;
          setFileBuffer(buffer);
        } catch (err) {
          console.error('Error downloading document:', err);
          throw new Error(`Failed to download document: ${err.message}`);
        }
      }
    } catch (error) {
      console.error('Error fetching document:', error);
    }
  };

  const toggleSidebar = () => {
    setOpenSidebar((prev) => !prev);
  };

  const handleCloseViewer = () => {
    setIsPdf(false);
    setIsExcel(false);
    setFileBuffer(null);
    setActiveRecordId(null);
  };

  // These functions are kept for consistency with the props contract
  const handleSearchQueryChange = (query: string): void => {
    setSearchQuery(query);
    if (!query.trim()) {
      setHasSearched(false);
    }
  };

  const handleTopKChange = (callback: (prevTopK: number) => number): void => {
    setTopK(callback);
  };

  // Group search results by record ID
  const groupedResults = useMemo(() => {
    const groups: Record<string, SearchResult[]> = {};

    searchResults.forEach((result) => {
      const recordId = result.metadata?.recordId;
      if (recordId) {
        if (!groups[recordId]) {
          groups[recordId] = [];
        }
        groups[recordId].push(result);
      }
    });

    return groups;
  }, [searchResults]);

  // Format date helper
  const formatDate = (dateString: string): string => {
    if (!dateString) return 'N/A';
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    } catch (e) {
      return 'N/A';
    }
  };

  if (loading) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          height: '100vh',
          width: '100%',
          bgcolor: theme.palette.background.default,
        }}
      >
        <LinearProgress
          sx={{
            height: 3,
            '& .MuiLinearProgress-bar': {
              transition: 'transform 0.4s linear',
            },
          }}
        />
        <Box
          sx={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            flex: 1,
            gap: 2,
            p: 3,
          }}
        >
          <CircularProgress size={32} thickness={3} sx={{ color: theme.palette.primary.main }} />
          <Typography
            variant="body1"
            color="text.secondary"
            fontWeight={500}
            letterSpacing="0.01em"
          >
            Loading search history...
          </Typography>
        </Box>
      </Box>
    );
  }

  if (error) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100vh',
          width: '100%',
          gap: 3,
          p: 3,
          bgcolor: theme.palette.background.default,
        }}
      >
        <Icon
          icon="eva:alert-circle-outline"
          style={{
            fontSize: 64,
            color: theme.palette.error.main,
            opacity: 0.9,
          }}
        />
        <Typography
          variant="h6"
          color="error"
          fontWeight={500}
          sx={{ fontFamily: "'Inter', system-ui, sans-serif" }}
        >
          {error}
        </Typography>
        <Typography
          variant="body2"
          color="text.secondary"
          sx={{
            textAlign: 'center',
            maxWidth: 400,
            mb: 2,
            fontFamily: "'Inter', system-ui, sans-serif",
            lineHeight: 1.6,
          }}
        >
          We couldn't load the search results. Please try again later.
        </Typography>
        <Button
          variant="outlined"
          startIcon={<Icon icon="eva:arrow-back-fill" />}
          onClick={() => navigate('/knowledge-base/search/history')}
          sx={{
            mt: 2,
            borderRadius: '8px',
            textTransform: 'none',
            fontWeight: 500,
            paddingX: 3,
            paddingY: 1,
            boxShadow: 'none',
            transition: 'all 0.2s ease-in-out',
            fontFamily: "'Inter', system-ui, sans-serif",
            '&:hover': {
              boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
              bgcolor: alpha(theme.palette.primary.main, 0.05),
            },
          }}
        >
         Search History
        </Button>
      </Box>
    );
  }

  return (
    <Box
      sx={{
        display: 'flex',
        overflow: 'hidden',
        bgcolor:
          theme.palette.mode === 'dark'
            ? alpha(theme.palette.background.default, 0.94)
            : alpha(theme.palette.background.default, 0.98),
        position: 'relative',
        height: '95vh',
        // mt:1,
        fontFamily: "'Inter', system-ui, -apple-system, sans-serif",
      }}
    >
      {/* Sidebar */}
      {/* <KnowledgeSearchSideBar
        sx={{
          height: '100%',
          zIndex: 1000,
          flexShrink: 0,
          boxShadow: 'none',
          borderRight: `1px solid ${alpha(theme.palette.divider, 0.08)}`,
        }}
        filters={filters}
        onFilterChange={handleFilterChange}
        openSidebar={openSidebar}
        onToggleSidebar={toggleSidebar}
      /> */}

      {/* Main Content Area */}
      <Box
        sx={{
          maxHeight: '90vh',
          width: openSidebar
            ? `calc(100% - ${SIDEBAR_COLLAPSED_WIDTH}px)`
            : `calc(100% - ${SIDEBAR_COLLAPSED_WIDTH}px)`,
          transition: 'width 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
          display: 'flex',
          position: 'relative',
        }}
      >
        {/* Search Results Component */}
        <Box
          sx={{
            width: isCitationViewerOpen ? '40%' : '100%',
            height: '100%',
            transition: 'width 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
            overflow: 'auto',
            maxHeight: '100%',
            display: 'flex',
            flexDirection: 'column',
            '&::-webkit-scrollbar': {
              width: '8px',
              height: '8px',
            },
            '&::-webkit-scrollbar-thumb': {
              backgroundColor: alpha(theme.palette.text.primary, 0.15),
              borderRadius: '4px',
              '&:hover': {
                backgroundColor: alpha(theme.palette.text.primary, 0.25),
              },
            },
            '&::-webkit-scrollbar-track': {
              backgroundColor: 'transparent',
            },
          }}
        >
          {/* Header Section */}
          <Box
            sx={{
              px: { xs: 3, md: 4 },
              py: { xs: 3, md: 2 },
              borderBottom: `1px solid ${alpha(theme.palette.divider, 0.05)}`,
              background: 'transparent',
              position: 'sticky',
              top: 0,
              zIndex: 20,
              backdropFilter: 'blur(8px)',
              backgroundColor:
                theme.palette.mode === 'dark'
                  ? alpha(theme.palette.background.default, 0.8)
                  : alpha(theme.palette.background.paper, 0.8),
            }}
          >
            <Box
              sx={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                maxWidth: '1300px',
                mx: 'auto',
              }}
            >
              <Box>
                <Button
                  variant="text"
                  startIcon={<Icon icon="eva:arrow-back-fill" />}
                  onClick={() => navigate('/knowledge-base/search/history')}
                  sx={{
                    borderRadius: '4px',
                    textTransform: 'none',
                    fontWeight: 500,
                    mb: 1,
                    pl: 0,
                    fontSize: '0.875rem',
                    color: alpha(theme.palette.text.primary, 0.7),
                    transition: 'all 0.2s',
                    fontFamily: "'Inter', -apple-system, system-ui, sans-serif",
                    '&:hover': {
                      backgroundColor: 'transparent',
                      color: theme.palette.primary.main,
                      pl: 0.5,
                    },
                  }}
                >
                   Search History
                </Button>
                <Typography
                  variant="h5"
                  sx={{
                    fontWeight: 600,
                    fontSize: '1.125rem',
                    letterSpacing: '-0.01em',
                    fontFamily: "'Inter', system-ui, sans-serif",
                    color: theme.palette.text.primary,
                  }}
                >
                  Search History Details
                </Typography>
              </Box>
              <Button
                variant="outlined"
                startIcon={<Icon icon="eva:refresh-outline" />}
                onClick={handleRepeatSearch}
                sx={{
                  borderRadius: '4px',
                  textTransform: 'none',
                  fontWeight: 500,
                  fontSize: '0.875rem',
                  px: 2,
                  py: 0.75,
                  borderColor: alpha(theme.palette.primary.main, 0.3),
                  color: theme.palette.primary.main,
                  backgroundColor: 'transparent',
                  fontFamily: "'Inter', system-ui, sans-serif",
                  '&:hover': {
                    borderColor: theme.palette.primary.main,
                    backgroundColor: alpha(theme.palette.primary.main, 0.04),
                  },
                }}
              >
                Run Search
              </Button>
            </Box>
          </Box>

          {/* Content Container with Padding */}
          <Box sx={{ px: 2, py:1, flex: 1 }}>
            {/* Search Query Metadata Card - Using the new component */}
            <SearchQueryMetadata
              searchQuery={searchQuery}
              searchDate={searchHistory?.createdAt || ''}
              citationCount={searchResults.length}
              isArchived={searchHistory?.isArchived}
              isShared={searchHistory?.isShared}
              formatDate={formatDate}
            />

            {/* Search Results Section */}
            <Box sx={{ mt: 3 }}>
              {/* Results Count Header */}
              {!loading && searchResults.length > 0 && (
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    mb: 2.5,
                    flexWrap: 'wrap',
                    gap: 2,
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <Icon
                      icon="eva:list-fill"
                      style={{
                        width: 18,
                        height: 18,
                        color: theme.palette.primary.main,
                        opacity: 0.9,
                      }}
                    />
                    <Typography
                      variant="h6"
                      sx={{
                        fontWeight: 600,
                        fontSize: '1rem',
                        letterSpacing: '-0.01em',
                      }}
                    >
                      Search Results ({searchResults.length})
                    </Typography>
                  </Box>

                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <Chip
                      icon={<Icon icon="eva:funnel-fill" style={{ fontSize: 14 }} />}
                      label={`Top ${searchHistory?.limit || topK} Results`}
                      size="small"
                      color="primary"
                      variant="outlined"
                      sx={{
                        height: 28,
                        fontSize: '0.75rem',
                        fontWeight: 600,
                        borderRadius: '8px',
                        pl: 0.5,
                      }}
                    />
                  </Box>
                </Box>
              )}

              {/* Empty Results */}
              {!loading && searchResults.length === 0 && (
                <Box
                  sx={{
                    textAlign: 'center',
                    py: 8,
                    px: 4,
                    mt: 3,
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    borderRadius: '12px',
                    border: `1px dashed ${alpha(theme.palette.divider, 0.15)}`,
                    bgcolor:
                      theme.palette.mode === 'dark'
                        ? alpha(theme.palette.background.paper, 0.2)
                        : alpha(theme.palette.background.paper, 0.5),
                  }}
                >
                  <Box
                    sx={{
                      width: 64,
                      height: 64,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      borderRadius: '50%',
                      bgcolor: alpha(theme.palette.warning.main, 0.08),
                      mb: 2.5,
                    }}
                  >
                    <Icon
                      icon="eva:file-search-outline"
                      style={{
                        fontSize: 28,
                        color: theme.palette.warning.main,
                        opacity: 0.8,
                      }}
                    />
                  </Box>
                  <Typography
                    variant="h6"
                    sx={{
                      mb: 1,
                      fontWeight: 600,
                      fontSize: '1.125rem',
                      letterSpacing: '-0.01em',
                    }}
                  >
                    No results found
                  </Typography>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{
                      mb: 3,
                      maxWidth: 500,
                      mx: 'auto',
                      lineHeight: 1.6,
                      fontSize: '0.875rem',
                    }}
                  >
                    This search history doesn't contain any results. The data may have been removed
                    or access restricted.
                  </Typography>
                  <Button
                    variant="outlined"
                    startIcon={<Icon icon="eva:refresh-outline" width={16} />}
                    onClick={handleRepeatSearch}
                    sx={{
                      borderRadius: '8px',
                      textTransform: 'none',
                      fontWeight: 500,
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      borderColor: alpha(theme.palette.primary.main, 0.4),
                    }}
                  >
                    Try Search Again
                  </Button>
                </Box>
              )}

              {/* Results by Record */}
              {Object.entries(groupedResults).map(([recordId, results], index) => {
                const record = recordsMap[recordId] || {};
                const recordName = record.recordName || 'Unknown Document';
                const isPdf = /\.pdf$/i.test(recordName) || record.extension === 'pdf';
                const isExcel =
                  /\.(xlsx|xls|csv)$/i.test(recordName) ||
                  ['xlsx', 'xls', 'csv'].includes(record.extension);

                const isActive = activeRecordId === recordId;

                return (
                  <Card
                    key={recordId}
                    elevation={0}
                    sx={{
                      mb: 3,
                      borderRadius: '12px',
                      overflow: 'hidden',
                      border: `1px solid ${
                        isActive
                          ? alpha(theme.palette.primary.main, 0.3)
                          : alpha(theme.palette.divider, 0.08)
                      }`,
                      boxShadow: isActive
                        ? `0 0 0 1px ${alpha(theme.palette.primary.main, 0.05)}, 0 4px 16px ${alpha(theme.palette.primary.main, 0.08)}`
                        : theme.palette.mode === 'dark'
                          ? 'none'
                          : `0 1px 6px ${alpha('#000', 0.03)}`,
                      transition: 'all 0.25s ease-in-out',
                      '&:hover': {
                        boxShadow: isActive
                          ? `0 0 0 1px ${alpha(theme.palette.primary.main, 0.05)}, 0 6px 20px ${alpha(theme.palette.primary.main, 0.12)}`
                          : theme.palette.mode === 'dark'
                            ? `0 4px 12px ${alpha('#000', 0.15)}`
                            : `0 4px 12px ${alpha('#000', 0.06)}`,
                      },
                    }}
                  >
                    <Box
                      sx={{
                        px: 3,
                        py: 2,
                        bgcolor: isActive
                          ? alpha(theme.palette.primary.main, 0.04)
                          : theme.palette.mode === 'dark'
                            ? alpha(theme.palette.background.paper, 0.6)
                            : alpha(theme.palette.background.paper, 0.8),
                        borderBottom: `1px solid ${
                          isActive
                            ? alpha(theme.palette.primary.main, 0.15)
                            : alpha(theme.palette.divider, 0.06)
                        }`,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                      }}
                    >
                      <Box>
                        <Typography
                          variant="subtitle1"
                          fontWeight={600}
                          letterSpacing="-0.01em"
                          sx={{
                            color: isActive ? 'primary.main' : 'text.primary',
                            fontSize: '0.9375rem',
                            lineHeight: 1.4,
                          }}
                        >
                          {recordName}
                        </Typography>
                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{
                            fontSize: '0.75rem',
                            display: 'flex',
                            alignItems: 'center',
                            gap: 0.5,
                          }}
                        >
                          <Icon icon="eva:file-text-outline" width={14} style={{ opacity: 0.7 }} />
                          {results.length} {results.length === 1 ? 'match' : 'matches'} found
                        </Typography>
                      </Box>
                      <Box>
                        {(isPdf || isExcel) && (
                          <Button
                            variant={isActive ? 'contained' : 'outlined'}
                            size="small"
                            onClick={() => viewCitations(recordId, isPdf, isExcel)}
                            startIcon={
                              <Icon
                                icon={isPdf ? 'eva:file-text-outline' : 'eva:grid-outline'}
                                width={16}
                                style={{
                                  color: isActive ? 'inherit' : isPdf ? '#f44336' : '#4caf50',
                                }}
                              />
                            }
                            sx={{
                              borderRadius: '8px',
                              textTransform: 'none',
                              fontWeight: 500,
                              fontSize: '0.8125rem',
                              py: 0.6,
                              boxShadow: isActive ? '0 2px 6px rgba(0,0,0,0.08)' : 'none',
                              borderColor: isActive
                                ? 'transparent'
                                : alpha(theme.palette.divider, 0.5),
                              transition: 'all 0.2s',
                              '&:hover': {
                                boxShadow: isActive ? '0 4px 12px rgba(0,0,0,0.15)' : 'none',
                                borderColor: isActive ? 'transparent' : theme.palette.divider,
                                bgcolor: isActive
                                  ? theme.palette.primary.main
                                  : alpha(theme.palette.background.default, 0.5),
                              },
                            }}
                          >
                            {isActive ? 'Viewing' : 'View Document'}
                          </Button>
                        )}
                      </Box>
                    </Box>
                    <CardContent
                      sx={{
                        p: 0,
                        bgcolor:
                          theme.palette.mode === 'dark'
                            ? alpha(theme.palette.background.default, 0.3)
                            : alpha(theme.palette.background.paper, 0.3),
                        '&:last-child': { pb: 0 },
                      }}
                    >
                      {results.map((result, rIndex) => (
                        <Box
                          key={result.metadata?._id || `result-${rIndex}`}
                          sx={{
                            position: 'relative',
                            transition: 'background-color 0.15s ease',
                            '&:hover': {
                              bgcolor:
                                theme.palette.mode === 'dark'
                                  ? alpha(theme.palette.background.paper, 0.15)
                                  : alpha(theme.palette.background.paper, 0.6),
                              '& .view-document-btn': {
                                opacity: 1,
                                visibility: 'visible',
                                transform: 'translateY(0)',
                              },
                            },
                          }}
                        >
                          <Box sx={{ p: 3 }}>
                            <SearchResultItem result={result} />

                            {/* View document button - visible on hover */}
                            {(isPdf || isExcel) && (
                              <Button
                                className="view-document-btn"
                                variant="outlined"
                                size="small"
                                onClick={() => viewCitations(recordId, isPdf, isExcel)}
                                startIcon={
                                  <Icon
                                    icon={isPdf ? 'eva:file-text-outline' : 'eva:grid-outline'}
                                    width={14}
                                    style={{
                                      color: isPdf ? '#f44336' : '#4caf50',
                                    }}
                                  />
                                }
                                sx={{
                                  position: 'absolute',
                                  top: 16,
                                  right: 16,
                                  opacity: 0,
                                  visibility: 'hidden',
                                  transform: 'translateY(4px)',
                                  transition:
                                    'opacity 0.2s ease, visibility 0.2s ease, transform 0.2s ease',
                                  textTransform: 'none',
                                  fontWeight: 500,
                                  borderRadius: '6px',
                                  fontSize: '0.75rem',
                                  py: 0.5,
                                  px: 1.5,
                                  boxShadow: `0 2px 8px ${alpha(theme.palette.background.default, 0.5)}`,
                                  borderColor: alpha(theme.palette.divider, 0.25),
                                  bgcolor: alpha(theme.palette.background.paper, 0.9),
                                  backdropFilter: 'blur(4px)',
                                  '&:hover': {
                                    borderColor: alpha(theme.palette.primary.main, 0.5),
                                    bgcolor: alpha(theme.palette.background.paper, 0.95),
                                  },
                                }}
                              >
                                View
                              </Button>
                            )}
                          </Box>
                          {rIndex < results.length - 1 && (
                            <Divider
                              sx={{
                                mx: 3,
                                borderStyle: 'dashed',
                                borderColor: alpha(theme.palette.divider, 0.3),
                              }}
                            />
                          )}
                        </Box>
                      ))}
                    </CardContent>
                  </Card>
                );
              })}
            </Box>
          </Box>
        </Box>

        {/* Add a subtle divider only when viewer is open */}
        {isCitationViewerOpen && (
          <Divider
            orientation="vertical"
            flexItem
            sx={{ borderColor: alpha(theme.palette.divider, 0.07) }}
          />
        )}

        {/* Document viewer placeholder when nothing is selected */}
        {/* {!isCitationViewerOpen && (
          <Box
            sx={{
              flex: 1,
              display: { xs: 'none', md: 'flex' },
              alignItems: 'center',
              justifyContent: 'center',
              bgcolor: theme.palette.mode === 'dark'
                ? alpha(theme.palette.background.paper, 0.05)
                : alpha(theme.palette.background.paper, 0.05),
              borderLeft: `1px solid ${alpha(theme.palette.divider, 0.07)}`,
            }}
          >
            <Paper
              elevation={0}
              sx={{
                p: 4,
                textAlign: 'center',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                maxWidth: 400,
                borderRadius: '16px',
                bgcolor: theme.palette.mode === 'dark'
                  ? alpha(theme.palette.background.paper, 0.2)
                  : alpha(theme.palette.background.paper, 0.8),
                border: `1px dashed ${alpha(theme.palette.primary.main, 0.15)}`,
                backdropFilter: 'blur(8px)',
                boxShadow: theme.palette.mode === 'dark'
                  ? 'none'
                  : `0 4px 24px ${alpha('#000', 0.03)}`
              }}
            >
              <Box
                sx={{
                  width: 60,
                  height: 60,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  borderRadius: '50%',
                  bgcolor: alpha(theme.palette.primary.main, 0.08),
                  mb: 2.5
                }}
              >
                <Icon
                  icon="eva:file-text-outline"
                  width={28}
                  style={{ 
                    color: theme.palette.primary.main,
                    opacity: 0.8 
                  }}
                />
              </Box>
              <Typography 
                variant="h6" 
                sx={{ 
                  mb: 1.5,
                  fontSize: '1.125rem',
                  fontWeight: 600,
                  letterSpacing: '-0.01em'
                }}
              >
                Select a document to view
              </Typography>
              <Typography 
                variant="body2" 
                color="text.secondary" 
                sx={{ 
                  mb: 3,
                  fontSize: '0.875rem',
                  lineHeight: 1.6
                }}
              >
                Click on "View Document" for any PDF or Excel file to see it with highlighted citations
              </Typography>
            </Paper>
          </Box>
        )} */}

        {/* PDF Viewer Container */}
        {isPdf && (
          <Box
            sx={{
              width: '60%',
              height: '100%',
              position: 'relative',
              display: 'flex',
              flexDirection: 'column',
              bgcolor: theme.palette.background.default,
              borderLeft: `1px solid ${alpha(theme.palette.divider, 0.07)}`,
            }}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                px: 2.5,
                py: 2,
                bgcolor:
                  theme.palette.mode === 'dark'
                    ? alpha(theme.palette.background.paper, 0.2)
                    : alpha(theme.palette.background.paper, 0.8),
                borderBottom: `1px solid ${alpha(theme.palette.divider, 0.08)}`,
                backdropFilter: 'blur(8px)',
                zIndex: 1,
              }}
            >
              <Typography
                variant="subtitle1"
                fontWeight={600}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  fontSize: '0.9375rem',
                }}
              >
                <Icon
                  icon="eva:file-text-outline"
                  width={18}
                  style={{
                    color: '#f44336',
                    opacity: 0.9,
                  }}
                />
                {activeRecordId && recordsMap[activeRecordId]?.recordName || 'Document Preview'}
              </Typography>
            </Box>
            <PdfHighlighterComp
              key="pdf-viewer"
              pdfUrl={fileUrl}
              pdfBuffer={fileBuffer || null}
              citations={recordCitations?.documents || []}
            />
          </Box>
        )}

        {isCitationViewerOpen && (
          <StyledCloseButton
            onClick={handleCloseViewer}
            startIcon={<Icon icon="mdi:close" />}
            size="small"
          >
            Close
          </StyledCloseButton>
        )}

        {/* Excel Viewer Container */}
        {isExcel && (
          <Box
            sx={{
              width: '60%',
              height: '100%',
              position: 'relative',
              display: 'flex',
              flexDirection: 'column',
              bgcolor: theme.palette.background.default,
              borderLeft: `1px solid ${alpha(theme.palette.divider, 0.07)}`,
            }}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                px: 2.5,
                py: 1,
                bgcolor:
                  theme.palette.mode === 'dark'
                    ? alpha(theme.palette.background.paper, 0.2)
                    : alpha(theme.palette.background.paper, 0.8),
                borderBottom: `1px solid ${alpha(theme.palette.divider, 0.08)}`,
                backdropFilter: 'blur(8px)',
                zIndex: 1,
              }}
            >
              <Typography
                variant="subtitle1"
                fontWeight={600}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  fontSize: '0.9375rem',
                }}
              >
                <Icon
                  icon="eva:grid-outline"
                  width={18}
                  style={{
                    color: '#4caf50',
                    opacity: 0.9,
                  }}
                />
                { activeRecordId && recordsMap[activeRecordId]?.recordName || 'Document Preview'}
              </Typography>

            </Box>
            <ExcelViewer
              key="excel-viewer"
              fileUrl={fileUrl}
              citations={recordCitations?.documents || []}
              excelBuffer={fileBuffer}
            />
          </Box>
        )}
      </Box>
    </Box>
  );
}
