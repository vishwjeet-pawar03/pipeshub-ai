import { format } from 'date-fns';
import { Icon } from '@iconify/react';
import { useNavigate } from 'react-router-dom';
import React, { useState, useEffect, useCallback } from 'react';

import { DataGrid } from '@mui/x-data-grid';
import {
  Box,
  Menu,
  Chip,
  alpha,
  Stack,
  Paper,
  Button,
  Dialog,
  Select,
  Tooltip,
  Divider,
  MenuItem,
  useTheme,
  TextField,
  Typography,
  IconButton,
  Pagination,
  DialogTitle,
  ListItemIcon,
  ListItemText,
  DialogContent,
  DialogActions,
  InputAdornment,
  LinearProgress,
  CircularProgress,
} from '@mui/material';

import axios from 'src/utils/axios';

// Types
interface SearchHistory {
  _id: string;
  query: string;
  limit: number;
  orgId: string;
  userId: string;
  citationIds: string[];
  records: Record<string, any>;
  isShared: boolean;
  isArchived: boolean;
  sharedWith: string[];
  createdAt: string;
  updatedAt: string;
}

interface SearchHistoryListResponse {
  searchHistory: SearchHistory[];
  pagination: {
    page: number;
    limit: number;
    totalCount: number;
    totalPages: number;
    hasNextPage: boolean;
    hasPrevPage: boolean;
  };
  filters: any;
  meta: {
    requestId: string;
    timestamp: string;
    duration: number;
  };
}

// API service functions
const apiService = {
  getSearchHistory: async (page = 1, limit = 10, query = '', showArchived = false) => {
    try {
      const params = new URLSearchParams();
      params.append('page', page.toString());
      params.append('limit', limit.toString());

      if (query) {
        params.append('search', query);
      }

      if (showArchived) {
        params.append('archived', 'true');
      }

      const response = await axios.get<SearchHistoryListResponse>(
        `/api/v1/search?${params.toString()}`
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching search history:', error);
      throw error;
    }
  },

  deleteSearchHistory: async (id: string) => {
    try {
      const response = await axios.delete(`/api/v1/search/${id}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting search history:', error);
      throw error;
    }
  },

  archiveSearchHistory: async (id: string) => {
    try {
      const response = await axios.post(`/api/v1/search/${id}/archive`);
      return response.data;
    } catch (error) {
      console.error('Error archiving search history:', error);
      throw error;
    }
  },

  unarchiveSearchHistory: async (id: string) => {
    try {
      const response = await axios.post(`/api/v1/search/${id}/unarchive`);
      return response.data;
    } catch (error) {
      console.error('Error unarchiving search history:', error);
      throw error;
    }
  },

  shareSearchHistory: async (id: string) => {
    try {
      const response = await axios.post(`/api/v1/search/${id}/share`);
      return response.data;
    } catch (error) {
      console.error('Error sharing search history:', error);
      throw error;
    }
  },

  unshareSearchHistory: async (id: string) => {
    try {
      const response = await axios.post(`/api/v1/search/${id}/unshare`);
      return response.data;
    } catch (error) {
      console.error('Error unsharing search history:', error);
      throw error;
    }
  },
};

// Helper function to truncate long text
const truncateText = (text: string, maxLength: number = 60) => {
  if (!text) return '';
  return text.length > maxLength ? `${text.substring(0, maxLength)}...` : text;
};

// Helper function to parse the record info
const parseRecordInfo = (recordStr: string) => {
  try {
    return JSON.parse(recordStr);
  } catch (error) {
    console.error('Error parsing record info:', error);
    return null;
  }
};

// Get file type icon based on extension
const getFileIcon = (extension: string | undefined) => {
  if (!extension) return 'mdi:file-question-outline';

  switch (extension.toLowerCase()) {
    case 'pdf':
      return 'mdi:file-pdf-box';
    case 'csv':
    case 'xlsx':
    case 'xls':
      return 'mdi:file-excel-box';
    case 'doc':
    case 'docx':
      return 'mdi:file-word-box';
    case 'ppt':
    case 'pptx':
      return 'mdi:file-powerpoint-box';
    case 'txt':
      return 'mdi:note-text-outline';
    case 'jpg':
    case 'jpeg':
    case 'png':
    case 'gif':
      return 'mdi:file-image-outline';
    case 'html':
    case 'htm':
      return 'mdi:language-html5';
    case 'css':
      return 'mdi:language-css3';
    case 'js':
    case 'ts':
      return 'mdi:language-javascript';
    case 'py':
      return 'mdi:language-python';
    case 'java':
      return 'mdi:language-java';
    case 'php':
      return 'mdi:language-php';
    case 'rb':
      return 'mdi:language-ruby';
    case 'c':
    case 'cpp':
      return 'mdi:language-c';
    case 'go':
      return 'mdi:language-go';
    case 'md':
      return 'mdi:language-markdown';
    case 'zip':
    case 'rar':
      return 'mdi:archive-outline';
    case 'mp3':
    case 'wav':
      return 'mdi:file-music-outline';
    case 'mp4':
    case 'avi':
    case 'mov':
      return 'mdi:file-video-outline';
    default:
      return 'mdi:file-document-outline';
  }
};

// Get file icon color based on extension
const getFileIconColor = (extension: string): string => {
  const ext = extension?.toLowerCase() || '';

  switch (ext) {
    case 'pdf':
      return '#f44336'; // Red
    case 'doc':
    case 'docx':
      return '#2196f3'; // Blue
    case 'xls':
    case 'xlsx':
    case 'csv':
      return '#4caf50'; // Green
    case 'ppt':
    case 'pptx':
      return '#ff9800'; // Orange
    case 'jpg':
    case 'jpeg':
    case 'png':
    case 'gif':
    case 'svg':
    case 'webp':
      return '#9c27b0'; // Purple
    case 'zip':
    case 'rar':
    case '7z':
      return '#795548'; // Brown
    case 'txt':
    case 'md':
      return '#607d8b'; // Blue Grey
    case 'html':
    case 'htm':
      return '#e65100'; // Deep Orange
    case 'css':
      return '#0277bd'; // Light Blue
    case 'js':
    case 'ts':
      return '#ffd600'; // Yellow
    case 'py':
      return '#1976d2'; // Blue
    case 'java':
      return '#b71c1c'; // Dark Red
    case 'php':
      return '#6a1b9a'; // Deep Purple
    case 'mp3':
    case 'wav':
      return '#283593'; // Indigo
    case 'mp4':
    case 'avi':
    case 'mov':
      return '#d81b60'; // Pink
    default:
      return '#1976d2'; // Default Blue
  }
};

// Main component
const KnowledgeSearchHistory = () => {
  const theme = useTheme();
  const navigate = useNavigate();

  // States
  const [searchHistory, setSearchHistory] = useState<SearchHistory[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [totalCount, setTotalCount] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [showArchived, setShowArchived] = useState(false);
  const [selectedHistory, setSelectedHistory] = useState<SearchHistory | null>(null);
  const [openDialog, setOpenDialog] = useState(false);
  const [dialogAction, setDialogAction] = useState<
    'delete' | 'archive' | 'unarchive' | 'share' | 'unshare'
  >('delete');
  const [isProcessing, setIsProcessing] = useState(false);

  // Action menu state
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const [activeHistoryId, setActiveHistoryId] = useState<string | null>(null);
  const menuOpen = Boolean(menuAnchorEl);

  // Fetch search history
  const fetchSearchHistory = useCallback(async () => {
    setIsLoading(true);
    try {
      const response = await apiService.getSearchHistory(
        page,
        rowsPerPage,
        searchQuery,
        showArchived
      );
      setSearchHistory(response.searchHistory);
      setTotalCount(response.pagination.totalCount);
      setTotalPages(response.pagination.totalPages);
      setIsInitialLoad(false);
    } catch (error) {
      console.error('Error fetching search history:', error);
    } finally {
      setIsLoading(false);
    }
  }, [page, rowsPerPage, searchQuery, showArchived]);

  // Effects
  useEffect(() => {
    fetchSearchHistory();
  }, [fetchSearchHistory]);

  // Search history handler
  const handleSearch = () => {
    setPage(1);
    fetchSearchHistory();
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  // Menu handlers
  const handleOpenMenu = (event: React.MouseEvent<HTMLButtonElement>, historyId: string) => {
    event.stopPropagation();
    setMenuAnchorEl(event.currentTarget);
    setActiveHistoryId(historyId);
  };

  const handleCloseMenu = () => {
    setMenuAnchorEl(null);
    setActiveHistoryId(null);
  };

  // Action handler from menu
  const handleActionFromMenu = (
    action: 'delete' | 'archive' | 'unarchive' | 'share' | 'unshare'
  ) => {
    if (!activeHistoryId) return;

    const history = searchHistory.find((h) => h._id === activeHistoryId);
    if (history) {
      openConfirmationDialog(action, history);
    }

    handleCloseMenu();
  };

  // Dialog handlers
  const openConfirmationDialog = (
    action: 'delete' | 'archive' | 'unarchive' | 'share' | 'unshare',
    history: SearchHistory
  ) => {
    setSelectedHistory(history);
    setDialogAction(action);
    setOpenDialog(true);
  };

  const handleCloseDialog = () => {
    setOpenDialog(false);
    setSelectedHistory(null);
  };

  // Action handlers
  const handleConfirmAction = async () => {
    if (!selectedHistory) return;

    setIsProcessing(true);
    try {
      switch (dialogAction) {
        case 'delete':
          await apiService.deleteSearchHistory(selectedHistory._id);
          break;
        case 'archive':
          await apiService.archiveSearchHistory(selectedHistory._id);
          break;
        case 'unarchive':
          await apiService.unarchiveSearchHistory(selectedHistory._id);
          break;
        case 'share':
          await apiService.shareSearchHistory(selectedHistory._id);
          break;
        case 'unshare':
          await apiService.unshareSearchHistory(selectedHistory._id);
          break;
      }
      // Refresh list after action
      fetchSearchHistory();
      handleCloseDialog();
    } catch (error) {
      console.error(`Error performing ${dialogAction} action:`, error);
    } finally {
      setIsProcessing(false);
    }
  };

  // Handle view details - navigate to search details page
  const handleViewDetails = (id: string) => {
    navigate(`/knowledge-base/search/history/${id}`);
  };

  // Handle repeat search
  const handleRepeatSearch = (query: string) => {
    navigate(`/knowledge-search?query=${encodeURIComponent(query)}`);
  };

  // Handle row click
  const handleRowClick = (params: any) => {
    handleViewDetails(params.id);
  };

  // DataGrid columns
  const columns = [
    {
      field: 'query',
      headerName: 'Search Query',
      flex: 1,
      minWidth: 250,
      renderCell: (params: any) => {
        const history = params.row;
        const recordKeys = Object.keys(history.records || {});
        const recordCount = recordKeys.length;

        // Parse first record if available
        let firstRecordInfo = null;
        if (recordCount > 0) {
          const firstRecordKey = recordKeys[0];
          const recordStr = history.records[firstRecordKey];
          firstRecordInfo = parseRecordInfo(recordStr);
        }

        return (
          <Box sx={{ 
            display: 'flex', 
            flexDirection: 'column', 
            py: 1, 
            width: '100%' 
          }}>
            <Typography
              variant="body2"
              fontWeight={500}
              sx={{
                fontSize: '0.875rem',
                lineHeight: 1.5,
                color: theme.palette.text.primary,
                mb: 0.5,
              }}
            >
              {truncateText(history.query, 100)}
            </Typography>

            {recordCount > 0 && firstRecordInfo && (
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <Icon
                  icon={getFileIcon(firstRecordInfo.extension)}
                  style={{ 
                    fontSize: 16, 
                    color: getFileIconColor(firstRecordInfo.extension),
                    marginRight: 8,
                    flexShrink: 0
                  }}
                />
                <Typography
                  variant="caption"
                  sx={{
                    color: theme.palette.text.secondary,
                    fontWeight: 500,
                    fontSize: '0.75rem',
                    maxWidth: 150,
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                  }}
                >
                  {firstRecordInfo.recordName || 'Unknown file'}
                </Typography>
              </Box>
            )}
          </Box>
        );
      },
    },
    {
      field: 'createdAt',
      headerName: 'Date',
      width: 140,
      renderCell: (params: any) => {
        const date = new Date(params.row.createdAt);
        return (
          <Box
            sx={{
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              justifyContent: 'center',
            }}
          >
            <Typography
              variant="body2"
              sx={{
                fontSize: '0.8125rem',
                fontWeight: 500,
                color: theme.palette.text.primary,
                lineHeight: 1.4,
              }}
            >
              {format(date, 'MMM d, yyyy')}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{
                fontSize: '0.75rem',
                lineHeight: 1.4,
              }}
            >
              {format(date, 'h:mm a')}
            </Typography>
          </Box>
        );
      },
    },
    {
      field: 'stats',
      headerName: 'Citations & Records',
      width: 160,
      align: 'center',
      headerAlign: 'center',
      renderCell: (params: any) => {
        const history = params.row;
        const recordCount = Object.keys(history.records || {}).length;
        const citationCount = history.citationIds?.length || 0;

        return (
          <Box
            sx={{
              width: '100%',
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              gap: 1.5,
              mt: 1.5,
            }}
          >
            <Chip
              label={citationCount}
              size="small"
              color="primary"
              variant="outlined"
              sx={{
                height: 24,
                fontSize: '0.75rem',
                fontWeight: 500,
                minWidth: 40,
                borderRadius: '6px',
              }}
            />

            <Chip
              label={recordCount}
              size="small"
              color="secondary"
              variant="outlined"
              sx={{
                height: 24,
                fontSize: '0.75rem',
                fontWeight: 500,
                minWidth: 40,
                borderRadius: '6px',
              }}
            />
          </Box>
        );
      },
    },
    {
      field: 'status',
      headerName: 'Status',
      width: 120,
      align: 'center',
      headerAlign: 'center',
      renderCell: (params: any) => {
        const history = params.row;
        let color: 'default' | 'primary' | 'success' | 'error' | 'info' | 'warning' = 'default';
        let label = 'Private';

        if (history.isArchived) {
          color = 'warning';
          label = 'Archived';
        } else if (history.isShared) {
          color = 'info';
          label = 'Shared';
        }

        return (
          <Box
            sx={{
              width: '100%',
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              mt: 1.5,
            }}
          >
            <Chip
              label={label}
              size="small"
              color={color}
              sx={{
                height: 24,
                minWidth: 70,
                fontSize: '0.75rem',
                fontWeight: 500,
                borderRadius: '6px',
              }}
            />
          </Box>
        );
      },
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 100,
      sortable: false,
      align: 'center',
      headerAlign: 'center',
      renderCell: (params: any) => {
        const history = params.row;
        
        const handleActionClick = (event: React.MouseEvent<HTMLButtonElement>) => {
          event.stopPropagation();
          handleOpenMenu(event, history._id);
        };

        return (
          <Box sx={{ width: '100%', display: 'flex', justifyContent: 'center', gap: 1, mt: 1 }}>
            <Tooltip title="Repeat search">
              <IconButton
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  handleRepeatSearch(history.query);
                }}
                sx={{
                  width: 28,
                  height: 28,
                  borderRadius: 1.5,
                  color: theme.palette.primary.main,
                  backgroundColor: alpha(theme.palette.primary.main, 0.08),
                  transition: 'all 0.15s ease-in-out',
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.primary.main, 0.16),
                    transform: 'translateY(-1px)',
                  },
                  '&:active': {
                    transform: 'translateY(0)',
                  },
                }}
              >
                <Icon icon="mdi:refresh" fontSize={18} />
              </IconButton>
            </Tooltip>
            
            <Tooltip title="Actions">
              <IconButton
                size="small"
                onClick={handleActionClick}
                sx={{
                  width: 28,
                  height: 28,
                  borderRadius: 1.5,
                  color: theme.palette.text.secondary,
                  backgroundColor: alpha(theme.palette.divider, 0.2),
                  transition: 'all 0.15s ease-in-out',
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.background.default, 0.8),
                    color: theme.palette.text.primary,
                    transform: 'translateY(-1px)',
                  },
                  '&:active': {
                    transform: 'translateY(0)',
                  },
                }}
              >
                <Icon icon="mdi:dots-vertical" fontSize={18} />
              </IconButton>
            </Tooltip>
          </Box>
        );
      },
    },
  ];

  // DataGrid page change handler
  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  // Handle rows per page change
  const handleRowsPerPageChange = (event: React.ChangeEvent<{ value: unknown }>) => {
    setRowsPerPage(Number(event.target.value));
    setPage(1);
  };

  // Render dialog content based on action
  const renderDialogContent = () => {
    if (!selectedHistory) return null;

    const actionTexts = {
      delete: {
        title: 'Delete Search History',
        message:
          'Are you sure you want to delete this search history? This action cannot be undone.',
        button: 'Delete',
        color: 'error',
        icon: 'mdi:trash-can-outline',
      },
      archive: {
        title: 'Archive Search History',
        message:
          'Are you sure you want to archive this search history? It will be moved to the archive and can be restored later.',
        button: 'Archive',
        color: 'primary',
        icon: 'mdi:archive-outline',
      },
      unarchive: {
        title: 'Unarchive Search History',
        message:
          'Are you sure you want to unarchive this search history? It will be restored to your active search history.',
        button: 'Unarchive',
        color: 'primary',
        icon: 'mdi:archive-arrow-up-outline',
      },
      share: {
        title: 'Share Search History',
        message: 'Are you sure you want to share this search history with your organization?',
        button: 'Share',
        color: 'primary',
        icon: 'mdi:share-variant',
      },
      unshare: {
        title: 'Unshare Search History',
        message: 'Are you sure you want to stop sharing this search history?',
        button: 'Unshare',
        color: 'primary',
        icon: 'mdi:share-off',
      },
    };

    const currentAction = actionTexts[dialogAction];

    return (
      <>
        <DialogTitle
          sx={{
            px: 3,
            py: 2.5,
            display: 'flex',
            alignItems: 'center',
            gap: 1.5,
            borderBottom: '1px solid',
            borderColor: alpha(theme.palette.divider, 0.08),
          }}
        >
          <Icon
            icon={currentAction.icon}
            style={{
              color:
                dialogAction === 'delete' ? theme.palette.error.main : theme.palette.primary.main,
              fontSize: 24,
            }}
          />
          <Typography variant="h6" fontWeight={600}>
            {currentAction.title}
          </Typography>
        </DialogTitle>
        <DialogContent sx={{ px: 3, py: 3 }}>
          <Typography
            variant="body1"
            sx={{
              mb: 3,
              fontSize: '0.9375rem',
              lineHeight: 1.6,
              color: alpha(theme.palette.text.primary, 0.8),
            }}
          >
            {currentAction.message}
          </Typography>
          <Paper
            elevation={0}
            variant="outlined"
            sx={{
              p: 2.5,
              mt: 1,
              mb: 2,
              borderRadius: '12px',
            }}
          >
            <Box
              sx={{
                p: 2,
                borderRadius: '8px',
                bgcolor: alpha(theme.palette.background.default, 0.5),
                fontFamily: "'SF Mono', 'Roboto Mono', monospace",
                fontSize: '0.875rem',
                color: theme.palette.text.primary,
                fontWeight: 500,
                lineHeight: 1.6,
                mb: 2,
                whiteSpace: 'pre-wrap',
              }}
            >
              {selectedHistory.query}
            </Box>

            <Box
              sx={{
                mt: 2,
                pt: 2,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                borderTop: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
              }}
            >
              <Box>
                <Typography
                  variant="caption"
                  color="text.secondary"
                  fontWeight={600}
                  sx={{
                    textTransform: 'uppercase',
                    fontSize: '0.6875rem',
                    letterSpacing: '0.03em',
                    display: 'block',
                    mb: 0.5,
                  }}
                >
                  Search Date
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    fontWeight: 500,
                  }}
                >
                  {format(new Date(selectedHistory.createdAt), 'MMM d, yyyy h:mm a')}
                </Typography>
              </Box>

              <Stack direction="row" spacing={2} alignItems="center">
                <Chip
                  icon={<Icon icon="mdi:file-document-multiple" style={{ fontSize: 14 }} />}
                  label={`${selectedHistory.citationIds?.length || 0} Citations`}
                  size="small"
                  color="primary"
                  variant="outlined"
                  sx={{
                    height: 28,
                    fontSize: '0.75rem',
                    fontWeight: 600,
                    borderRadius: '8px',
                  }}
                />

                <Chip
                  icon={<Icon icon="mdi:folder-multiple" style={{ fontSize: 14 }} />}
                  label={`${Object.keys(selectedHistory.records || {}).length} Records`}
                  size="small"
                  color="secondary"
                  variant="outlined"
                  sx={{
                    height: 28,
                    fontSize: '0.75rem',
                    fontWeight: 600,
                    borderRadius: '8px',
                  }}
                />
              </Stack>
            </Box>
          </Paper>
        </DialogContent>
        <DialogActions
          sx={{
            px: 3,
            py: 2.5,
            borderTop: '1px solid',
            borderColor: alpha(theme.palette.divider, 0.08),
          }}
        >
          <Button
            onClick={handleCloseDialog}
            disabled={isProcessing}
            size="medium"
            variant="outlined"
            color="inherit"
            sx={{
              borderRadius: '8px',
              textTransform: 'none',
              fontWeight: 500,
              px: 2,
            }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleConfirmAction}
            variant="contained"
            color={dialogAction === 'delete' ? 'error' : 'primary'}
            autoFocus
            disabled={isProcessing}
            startIcon={
              isProcessing ? <CircularProgress size={20} /> : <Icon icon={currentAction.icon} />
            }
            sx={{
              ml: 1.5,
              borderRadius: '8px',
              textTransform: 'none',
              fontWeight: 600,
              px: 2,
            }}
          >
            {isProcessing ? `${currentAction.button}ing...` : currentAction.button}
          </Button>
        </DialogActions>
      </>
    );
  };

  return (
    <Box
      sx={{
        height: '90vh',
        width: '100%',
        px: 4,
        mb:2,
        mt:2,
      }}
    >
      {/* Header section */}
      <Paper
        elevation={0}
        sx={{
          mb: 2.5,
          borderRadius: '12px',
        }}
      >
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Typography variant="h5" fontWeight={600} color="text.primary">
            Knowledge Search History
          </Typography>
          <Stack direction="row" spacing={1.5} alignItems="center">
            <Button
              variant="outlined"
              startIcon={<Icon icon="mdi:arrow-left" />}
              onClick={() => navigate('/knowledge-search')}
              sx={{
                borderRadius: '8px',
                textTransform: 'none',
                fontWeight: 600,
                fontSize: '0.875rem',
              }}
            >
              Back to Search
            </Button>
          </Stack>
        </Stack>
      </Paper>

      {/* Search and Filter Controls */}
      <Paper
        elevation={0}
        variant="outlined"
        sx={{
          p: 2.5,
          mb: 2.5,
          display: 'flex',
          flexDirection: { xs: 'column', sm: 'row' },
          gap: 2,
          alignItems: { xs: 'stretch', sm: 'center' },
          justifyContent: 'space-between',
          borderRadius: '12px',
        }}
      >
        <Box
          sx={{
            display: 'flex',
            gap: 2,
            flexGrow: 1,
            flexDirection: { xs: 'column', sm: 'row' },
            alignItems: 'center',
            width: { xs: '100%', sm: 'auto' },
          }}
        >
          <TextField
            placeholder="Search in history..."
            size="small"
            fullWidth
            sx={{
              maxWidth: { sm: 320 },
              '& .MuiOutlinedInput-root': {
                borderRadius: '8px',
                fontSize: '0.875rem',
                bgcolor: theme.palette.mode === 'dark'
                  ? alpha(theme.palette.background.default, 0.5)
                  : alpha(theme.palette.grey[50], 0.8),
                transition: 'all 0.2s',
                '&:hover': {
                  bgcolor: theme.palette.mode === 'dark'
                    ? alpha(theme.palette.background.default, 0.7)
                    : theme.palette.grey[50],
                },
                '&.Mui-focused': {
                  boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
                },
              },
            }}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={handleKeyPress}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Icon
                    icon="mdi:magnify"
                    style={{ fontSize: 20, color: theme.palette.text.secondary }}
                  />
                </InputAdornment>
              ),
            }}
          />
          <Button
            variant="contained"
            onClick={handleSearch}
            disabled={isLoading && !isInitialLoad}
            startIcon={
              isLoading && !isInitialLoad ? (
                <CircularProgress size={18} color="inherit" />
              ) : (
                <Icon icon="mdi:magnify" width={18} />
              )
            }
            sx={{
              minWidth: 100,
              height: 40,
              borderRadius: '8px',
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '0.875rem',
              boxShadow: theme.shadows[2],
              '&:hover': {
                boxShadow: theme.shadows[3],
              },
            }}
          >
            Search
          </Button>
        </Box>

        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant={showArchived ? 'contained' : 'outlined'}
            color={showArchived ? 'primary' : 'inherit'}
            startIcon={
              <Icon icon={showArchived ? 'mdi:archive-check-outline' : 'mdi:archive-outline'} />
            }
            onClick={() => {
              setShowArchived(!showArchived);
              setPage(1);
            }}
            sx={{
              height: 40,
              borderRadius: '8px',
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '0.875rem',
              px: 2.5,
              boxShadow: showArchived ? theme.shadows[1] : 'none',
              borderColor: showArchived ? 'transparent' : alpha(theme.palette.divider, 0.3),
              transition: 'all 0.2s',
              '&:hover': {
                boxShadow: showArchived ? theme.shadows[2] : 'none',
                borderColor: showArchived
                  ? 'transparent'
                  : alpha(theme.palette.primary.main, 0.5),
                bgcolor: showArchived
                  ? theme.palette.primary.dark
                  : alpha(theme.palette.primary.main, 0.04),
              },
            }}
          >
            {showArchived ? 'Showing Archived' : 'Show Archived'}
          </Button>
        </Box>
      </Paper>

      {/* Loading state */}
      {isLoading && isInitialLoad && (
        <LinearProgress
          sx={{
            mb: 2.5,
            height: 4,
            borderRadius: 2,
            background: alpha(theme.palette.primary.main, 0.1),
            '.MuiLinearProgress-bar': {
              background: theme.palette.primary.main,
            },
          }}
        />
      )}

      {/* Results Area */}
      {!isLoading && searchHistory.length === 0 ? (
        <Paper
          elevation={0}
          variant="outlined"
          sx={{
            py: 10,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            textAlign: 'center',
            borderRadius: '12px',
            height: 'calc(100vh - 220px)',
            px: 4,
          }}
        >
          <Box
            sx={{
              width: 80,
              height: 80,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              borderRadius: '16px',
              bgcolor: alpha(theme.palette.primary.main, 0.08),
              mb: 3,
              color: theme.palette.primary.main,
            }}
          >
            <Icon icon="mdi:magnify" style={{ fontSize: 36 }} />
          </Box>
          <Typography
            variant="h5"
            fontWeight={700}
            sx={{
              mb: 1.5,
              color: theme.palette.text.primary,
            }}
          >
            No search history found
          </Typography>
          <Typography
            variant="body1"
            color="text.secondary"
            sx={{
              maxWidth: 500,
              mb: 4,
              fontSize: '0.9375rem',
              lineHeight: 1.6,
            }}
          >
            {showArchived
              ? "You don't have any archived searches. Try switching to active searches or perform new queries."
              : 'Perform some knowledge searches to see your search history here.'}
          </Typography>
          <Button
            variant="contained"
            startIcon={<Icon icon="mdi:search" />}
            onClick={() => navigate('/knowledge-search')}
            sx={{
              borderRadius: '8px',
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '0.9375rem',
              px: 3,
              py: 1.25,
              boxShadow: theme.shadows[2],
              '&:hover': {
                boxShadow: theme.shadows[3],
              },
            }}
          >
            Go to Search
          </Button>
        </Paper>
      ) : (
        <Paper
          elevation={0}
          variant="outlined"
          sx={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            borderRadius: '12px',
            overflow: 'auto',
            minHeight: '70vh',
          }}
        >
          <Box sx={{ flexGrow: 1, }}>
            <DataGrid
              rows={searchHistory}
              columns={columns}
              hideFooter
              getRowId={(row) => row._id}
              onRowClick={handleRowClick}
              loading={isLoading}
              disableRowSelectionOnClick
              rowHeight={66}
              sx={{
                border: 'none',
                height: '100%',
                '& .MuiDataGrid-columnHeaders': {
                  backgroundColor: alpha('#000', 0.02),
                  borderBottom: '1px solid',
                  borderColor: 'divider',
                  minHeight: '56px !important',
                  height: '56px !important',
                  maxHeight: '56px !important',
                  lineHeight: '56px !important',
                },
                '& .MuiDataGrid-columnHeader': {
                  height: '56px !important',
                  maxHeight: '56px !important',
                  lineHeight: '56px !important',
                },
                '& .MuiDataGrid-columnHeaderTitle': {
                  fontWeight: 600,
                  fontSize: '0.875rem',
                  color: 'text.primary',
                },
                '& .MuiDataGrid-cell': {
                  border: 'none',
                  padding: 0,
                  maxHeight: '66px !important',
                  minHeight: '66px !important',
                  height: '66px !important',
                  lineHeight: '66px !important',
                },
                '& .MuiDataGrid-cellContent': {
                  maxHeight: '66px !important',
                  height: '66px !important',
                  lineHeight: '66px !important',
                },
                '& .MuiDataGrid-row': {
                  maxHeight: '66px !important',
                  minHeight: '66px !important',
                  height: '66px !important',
                  borderBottom: '1px solid',
                  borderColor: alpha('#000', 0.05),
                  paddingX:'12px',
                  '&:hover': {
                    backgroundColor: alpha('#1976d2', 0.03),
                    cursor: 'pointer',
                  },
                },
                '& .MuiDataGrid-cell:focus, .MuiDataGrid-cell:focus-within': {
                  outline: 'none',
                },
                '& .MuiDataGrid-columnHeader:focus, .MuiDataGrid-columnHeader:focus-within': {
                  outline: 'none',
                },
              }}
            />
          </Box>
          
          {/* Pagination footer */}
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              px: 3,
              py: 2,
              borderTop: '1px solid',
              borderColor: alpha('#000', 0.05),
              bgcolor: alpha('#000', 0.01),
              height: '64px',
            }}
          >
            <Typography variant="body2" color="text.secondary">
              {totalCount === 0
                ? 'No records found'
                : `Showing ${(page - 1) * rowsPerPage + 1}-${Math.min(
                    page * rowsPerPage,
                    totalCount
                  )} of ${totalCount} records`}
            </Typography>

            <Stack direction="row" spacing={2} alignItems="center">
              <Pagination
                count={totalPages}
                page={page}
                onChange={(event, value) => handlePageChange(value)}
                color="primary"
                size="small"
                shape="rounded"
                sx={{
                  '& .MuiPaginationItem-root': {
                    borderRadius: '6px',
                  },
                }}
              />
              <Select
                value={rowsPerPage}
                onChange={(e) => handleRowsPerPageChange(e as React.ChangeEvent<{ value: unknown }>)}
                size="small"
                sx={{
                  minWidth: 120,
                  height: 32,
                  '& .MuiOutlinedInput-root': {
                    borderRadius: '6px',
                  },
                }}
              >
                <MenuItem value={10}>10 per page</MenuItem>
                <MenuItem value={20}>20 per page</MenuItem>
                <MenuItem value={50}>50 per page</MenuItem>
                <MenuItem value={100}>100 per page</MenuItem>
              </Select>
            </Stack>
          </Box>
        </Paper>
      )}

      {/* Actions Menu */}
      <Menu
        id="actions-menu"
        anchorEl={menuAnchorEl}
        open={menuOpen}
        onClose={handleCloseMenu}
        onClick={(e) => e.stopPropagation()}
        MenuListProps={{
          'aria-labelledby': `actions-button-${activeHistoryId}`,
        }}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        PaperProps={{
          elevation: 2,
          sx: {
            mt: 0.5,
            overflow: 'visible',
            minWidth: 180,
            borderRadius: 2,
            boxShadow: '0 6px 16px rgba(0,0,0,0.08)',
            border: '1px solid',
            borderColor: 'rgba(0,0,0,0.04)',
            backdropFilter: 'blur(8px)',
            '.MuiList-root': {
              py: 0.75,
            },
            '&:before': {
              content: '""',
              display: 'block',
              position: 'absolute',
              top: 0,
              right: 14,
              width: 10,
              height: 10,
              bgcolor: 'background.paper',
              transform: 'translateY(-50%) rotate(45deg)',
              zIndex: 0,
            },
          },
        }}
      >
        <MenuItem 
          onClick={() => {
            if (activeHistoryId) {
              const history = searchHistory.find((h) => h._id === activeHistoryId);
              if (history) {
                handleViewDetails(history._id);
              }
            }
          }}
          sx={{
            py: 0.75,
            mx: 0.75,
            my: 0.25,
            px: 1.5,
            borderRadius: 1.5,
            transition: 'all 0.15s ease',
            '&:hover': {
              bgcolor: (theme) =>
                theme.palette.mode === 'dark'
                  ? alpha('#fff', 0.06)
                  : alpha('#000', 0.04),
              transform: 'translateX(2px)',
            },
          }}
        >
          <ListItemIcon sx={{ minWidth: 30, color: theme.palette.info.main }}>
            <Icon icon="mdi:eye-outline" width={18} height={18} />
          </ListItemIcon>
          <ListItemText 
            primary="View Details"
            primaryTypographyProps={{
              variant: 'body2',
              fontWeight: 500,
              fontSize: '0.875rem',
            }}
          />
        </MenuItem>
        
        <MenuItem 
          onClick={() => {
            if (activeHistoryId) {
              const history = searchHistory.find((h) => h._id === activeHistoryId);
              if (history) {
                handleRepeatSearch(history.query);
              }
            }
          }}
          sx={{
            py: 0.75,
            mx: 0.75,
            my: 0.25,
            px: 1.5,
            borderRadius: 1.5,
            transition: 'all 0.15s ease',
            '&:hover': {
              bgcolor: (theme) =>
                theme.palette.mode === 'dark'
                  ? alpha('#fff', 0.06)
                  : alpha('#000', 0.04),
              transform: 'translateX(2px)',
            },
          }}
        >
          <ListItemIcon sx={{ minWidth: 30, color: theme.palette.primary.main }}>
            <Icon icon="mdi:refresh" width={18} height={18} />
          </ListItemIcon>
          <ListItemText 
            primary="Repeat Search"
            primaryTypographyProps={{
              variant: 'body2',
              fontWeight: 500,
              fontSize: '0.875rem',
            }}
          />
        </MenuItem>

        {activeHistoryId && (
          <>
            {searchHistory.find((h) => h._id === activeHistoryId)?.isShared ? (
              <MenuItem 
                onClick={() => handleActionFromMenu('unshare')}
                sx={{
                  py: 0.75,
                  mx: 0.75,
                  my: 0.25,
                  px: 1.5,
                  borderRadius: 1.5,
                  transition: 'all 0.15s ease',
                  '&:hover': {
                    bgcolor: (theme) =>
                      theme.palette.mode === 'dark'
                        ? alpha('#fff', 0.06)
                        : alpha('#000', 0.04),
                    transform: 'translateX(2px)',
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 30, color: theme.palette.info.main }}>
                  <Icon icon="mdi:share-off" width={18} height={18} />
                </ListItemIcon>
                <ListItemText 
                  primary="Unshare"
                  primaryTypographyProps={{
                    variant: 'body2',
                    fontWeight: 500,
                    fontSize: '0.875rem',
                  }}
                />
              </MenuItem>
            ) : (
              <MenuItem 
                onClick={() => handleActionFromMenu('share')}
                sx={{
                  py: 0.75,
                  mx: 0.75,
                  my: 0.25,
                  px: 1.5,
                  borderRadius: 1.5,
                  transition: 'all 0.15s ease',
                  '&:hover': {
                    bgcolor: (theme) =>
                      theme.palette.mode === 'dark'
                        ? alpha('#fff', 0.06)
                        : alpha('#000', 0.04),
                    transform: 'translateX(2px)',
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 30, color: theme.palette.info.main }}>
                  <Icon icon="mdi:share-variant" width={18} height={18} />
                </ListItemIcon>
                <ListItemText 
                  primary="Share"
                  primaryTypographyProps={{
                    variant: 'body2',
                    fontWeight: 500,
                    fontSize: '0.875rem',
                  }}
                />
              </MenuItem>
            )}

            {searchHistory.find((h) => h._id === activeHistoryId)?.isArchived ? (
              <MenuItem 
                onClick={() => handleActionFromMenu('unarchive')}
                sx={{
                  py: 0.75,
                  mx: 0.75,
                  my: 0.25,
                  px: 1.5,
                  borderRadius: 1.5,
                  transition: 'all 0.15s ease',
                  '&:hover': {
                    bgcolor: (theme) =>
                      theme.palette.mode === 'dark'
                        ? alpha('#fff', 0.06)
                        : alpha('#000', 0.04),
                    transform: 'translateX(2px)',
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 30, color: theme.palette.warning.main }}>
                  <Icon icon="mdi:archive-arrow-up-outline" width={18} height={18} />
                </ListItemIcon>
                <ListItemText 
                  primary="Unarchive"
                  primaryTypographyProps={{
                    variant: 'body2',
                    fontWeight: 500,
                    fontSize: '0.875rem',
                  }}
                />
              </MenuItem>
            ) : (
              <MenuItem 
                onClick={() => handleActionFromMenu('archive')}
                sx={{
                  py: 0.75,
                  mx: 0.75,
                  my: 0.25,
                  px: 1.5,
                  borderRadius: 1.5,
                  transition: 'all 0.15s ease',
                  '&:hover': {
                    bgcolor: (theme) =>
                      theme.palette.mode === 'dark'
                        ? alpha('#fff', 0.06)
                        : alpha('#000', 0.04),
                    transform: 'translateX(2px)',
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 30, color: theme.palette.warning.main }}>
                  <Icon icon="mdi:archive-outline" width={18} height={18} />
                </ListItemIcon>
                <ListItemText 
                  primary="Archive"
                  primaryTypographyProps={{
                    variant: 'body2',
                    fontWeight: 500,
                    fontSize: '0.875rem',
                  }}
                />
              </MenuItem>
            )}
          </>
        )}

        <Divider sx={{ my: 0.75, opacity: 0.6 }} />

        <MenuItem 
          onClick={() => handleActionFromMenu('delete')}
          sx={{
            py: 0.75,
            mx: 0.75,
            my: 0.25,
            px: 1.5,
            borderRadius: 1.5,
            color: 'error.main',
            transition: 'all 0.15s ease',
            '&:hover': {
              bgcolor: 'error.lighter',
              transform: 'translateX(2px)',
            },
          }}
        >
          <ListItemIcon sx={{ minWidth: 30, color: theme.palette.error.main }}>
            <Icon icon="mdi:trash-can-outline" width={18} height={18} />
          </ListItemIcon>
          <ListItemText 
            primary="Delete"
            primaryTypographyProps={{
              variant: 'body2',
              fontWeight: 500,
              fontSize: '0.875rem',
            }}
          />
        </MenuItem>
      </Menu>

      {/* Confirmation Dialog */}
      <Dialog
        open={openDialog}
        onClose={handleCloseDialog}
        PaperProps={{
          elevation: 3,
          sx: {
            borderRadius: '12px',
            maxWidth: 500,
            overflow: 'hidden',
            boxShadow: '0 8px 32px rgba(0,0,0,0.1)',
          },
        }}
      >
        {renderDialogContent()}
      </Dialog>
    </Box>
  );
};

export default KnowledgeSearchHistory;